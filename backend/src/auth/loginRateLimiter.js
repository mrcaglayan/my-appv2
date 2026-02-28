import crypto from "node:crypto";
import { createClient } from "redis";
import { logWarn } from "../observability/logger.js";

const LOGIN_RATE_WINDOW_MS = parsePositiveIntEnv(
  process.env.AUTH_LOGIN_RATE_WINDOW_MS,
  15 * 60 * 1000
);
const LOGIN_RATE_MAX_ATTEMPTS = parsePositiveIntEnv(
  process.env.AUTH_LOGIN_RATE_MAX_ATTEMPTS,
  10
);
const LOGIN_RATE_BLOCK_MS = parsePositiveIntEnv(
  process.env.AUTH_LOGIN_RATE_BLOCK_MS,
  15 * 60 * 1000
);
const LOGIN_RATE_MAX_TRACKED_KEYS = parsePositiveIntEnv(
  process.env.AUTH_LOGIN_RATE_MAX_TRACKED_KEYS,
  10_000
);
const LOGIN_RATE_PRUNE_INTERVAL_MS = 60 * 1000;
const LOGIN_RATE_STORE_MODE = normalizeStoreMode(process.env.AUTH_LOGIN_RATE_STORE);
const LOGIN_RATE_REDIS_URL = String(
  process.env.AUTH_LOGIN_RATE_REDIS_URL || process.env.REDIS_URL || ""
).trim();
const LOGIN_RATE_REDIS_CONNECT_TIMEOUT_MS = parsePositiveIntEnv(
  process.env.AUTH_LOGIN_RATE_REDIS_CONNECT_TIMEOUT_MS,
  2_000
);
const REDIS_LOG_COOLDOWN_MS = 60 * 1000;

const registerFailureLuaScript = `
local failKey = KEYS[1]
local blockKey = KEYS[2]

local windowMs = tonumber(ARGV[1])
local maxAttempts = tonumber(ARGV[2])
local blockMs = tonumber(ARGV[3])

local function ceilSeconds(ms)
  return math.floor((ms + 999) / 1000)
end

local blockTtl = redis.call('PTTL', blockKey)
if blockTtl > 0 then
  return {1, ceilSeconds(blockTtl), -1}
end

local failedCount = redis.call('INCR', failKey)
if failedCount == 1 then
  redis.call('PEXPIRE', failKey, windowMs)
end

if failedCount >= maxAttempts then
  redis.call('SET', blockKey, '1', 'PX', blockMs)
  redis.call('DEL', failKey)
  return {1, ceilSeconds(blockMs), failedCount}
end

return {0, 0, failedCount}
`;

const memoryLimiterState = new Map();
let lastMemoryLimiterPruneAt = 0;
let resolvedStoreBackend = null; // "redis" | "memory"
let redisClient = null;
let redisConnectPromise = null;
let lastRedisErrorLogAt = 0;

function parsePositiveIntEnv(value, fallback) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function normalizeStoreMode(value) {
  const normalized = String(value || "auto")
    .trim()
    .toLowerCase();
  if (["auto", "redis", "memory"].includes(normalized)) {
    return normalized;
  }
  return "auto";
}

function toInteger(value, fallback = 0) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.trunc(parsed);
}

function shouldAttemptRedis() {
  if (LOGIN_RATE_STORE_MODE === "memory") {
    return false;
  }
  return Boolean(LOGIN_RATE_REDIS_URL);
}

function buildRateLimitRedisKey(prefix, rateKey) {
  const digest = crypto.createHash("sha256").update(String(rateKey)).digest("hex");
  return `auth:login:${prefix}:${digest}`;
}

function logRedisError(message, err = null) {
  const now = Date.now();
  if (now - lastRedisErrorLogAt < REDIS_LOG_COOLDOWN_MS) {
    return;
  }
  lastRedisErrorLogAt = now;
  logWarn("[auth-rate-limit] Redis warning", { detail: message }, err || null);
}

async function connectRedisClient() {
  if (!shouldAttemptRedis()) {
    return null;
  }

  if (redisClient?.isOpen) {
    return redisClient;
  }

  if (!redisConnectPromise) {
    redisConnectPromise = (async () => {
      const client = createClient({
        url: LOGIN_RATE_REDIS_URL,
        socket: {
          connectTimeout: LOGIN_RATE_REDIS_CONNECT_TIMEOUT_MS,
          reconnectStrategy: () => false,
        },
      });
      client.on("error", (err) => {
        logRedisError("Redis client error. Falling back to in-memory limiter.", err);
      });

      try {
        await client.connect();
        redisClient = client;
        return redisClient;
      } catch (err) {
        try {
          if (client.isOpen) {
            await client.quit();
          }
        } catch {
          // Ignore redis disconnect failures.
        }
        logRedisError(
          "Could not connect to Redis. Falling back to in-memory limiter.",
          err
        );
        return null;
      }
    })();
  }

  try {
    return await redisConnectPromise;
  } finally {
    redisConnectPromise = null;
  }
}

function pruneMemoryLimiterState(now = Date.now()) {
  if (
    now - lastMemoryLimiterPruneAt < LOGIN_RATE_PRUNE_INTERVAL_MS &&
    memoryLimiterState.size <= LOGIN_RATE_MAX_TRACKED_KEYS
  ) {
    return;
  }
  lastMemoryLimiterPruneAt = now;

  for (const [key, state] of memoryLimiterState.entries()) {
    const blockExpired = Number(state.blockedUntil || 0) <= now;
    const windowExpired = now - Number(state.windowStartedAt || 0) > LOGIN_RATE_WINDOW_MS * 2;
    if (blockExpired && windowExpired) {
      memoryLimiterState.delete(key);
    }
  }

  if (memoryLimiterState.size > LOGIN_RATE_MAX_TRACKED_KEYS) {
    const overflow = memoryLimiterState.size - LOGIN_RATE_MAX_TRACKED_KEYS;
    const orderedKeys = Array.from(memoryLimiterState.entries())
      .sort((a, b) => Number(a[1]?.updatedAt || 0) - Number(b[1]?.updatedAt || 0))
      .slice(0, overflow)
      .map(([key]) => key);

    for (const key of orderedKeys) {
      memoryLimiterState.delete(key);
    }
  }
}

function getOrCreateMemoryRateLimitState(rateKey, now = Date.now()) {
  pruneMemoryLimiterState(now);

  const existing = memoryLimiterState.get(rateKey);
  if (!existing) {
    const created = {
      failedAttempts: 0,
      windowStartedAt: now,
      blockedUntil: 0,
      updatedAt: now,
    };
    memoryLimiterState.set(rateKey, created);
    return created;
  }

  const isWindowExpired = now - Number(existing.windowStartedAt || 0) >= LOGIN_RATE_WINDOW_MS;
  const isBlockExpired = Number(existing.blockedUntil || 0) <= now;
  if (isWindowExpired && isBlockExpired) {
    existing.failedAttempts = 0;
    existing.windowStartedAt = now;
  }
  existing.updatedAt = now;
  return existing;
}

function getRateLimitBlockInfoMemory(rateKey, now = Date.now()) {
  const state = getOrCreateMemoryRateLimitState(rateKey, now);
  if (Number(state.blockedUntil || 0) <= now) {
    return { blocked: false, retryAfterSeconds: 0 };
  }
  return {
    blocked: true,
    retryAfterSeconds: Math.max(
      1,
      Math.ceil((Number(state.blockedUntil) - now) / 1000)
    ),
  };
}

function registerFailedLoginAttemptMemory(rateKey, now = Date.now()) {
  const state = getOrCreateMemoryRateLimitState(rateKey, now);
  if (Number(state.blockedUntil || 0) > now) {
    return {
      blocked: true,
      retryAfterSeconds: Math.max(
        1,
        Math.ceil((Number(state.blockedUntil) - now) / 1000)
      ),
    };
  }

  state.failedAttempts = Number(state.failedAttempts || 0) + 1;
  state.updatedAt = now;
  if (state.failedAttempts >= LOGIN_RATE_MAX_ATTEMPTS) {
    state.blockedUntil = now + LOGIN_RATE_BLOCK_MS;
    return {
      blocked: true,
      retryAfterSeconds: Math.max(1, Math.ceil(LOGIN_RATE_BLOCK_MS / 1000)),
    };
  }

  return { blocked: false, retryAfterSeconds: 0 };
}

function clearFailedLoginAttemptsMemory(rateKey) {
  memoryLimiterState.delete(rateKey);
}

async function getRateLimitBlockInfoRedis(rateKey) {
  const client = await connectRedisClient();
  if (!client) {
    return null;
  }

  const blockKey = buildRateLimitRedisKey("block", rateKey);
  const ttlMs = await client.pTTL(blockKey);
  if (toInteger(ttlMs, -2) > 0) {
    return {
      blocked: true,
      retryAfterSeconds: Math.max(1, Math.ceil(toInteger(ttlMs, 0) / 1000)),
    };
  }
  if (toInteger(ttlMs, -2) === -1) {
    return {
      blocked: true,
      retryAfterSeconds: Math.max(1, Math.ceil(LOGIN_RATE_BLOCK_MS / 1000)),
    };
  }
  return { blocked: false, retryAfterSeconds: 0 };
}

async function registerFailedLoginAttemptRedis(rateKey) {
  const client = await connectRedisClient();
  if (!client) {
    return null;
  }

  const failKey = buildRateLimitRedisKey("fail", rateKey);
  const blockKey = buildRateLimitRedisKey("block", rateKey);
  const result = await client.eval(registerFailureLuaScript, {
    keys: [failKey, blockKey],
    arguments: [
      String(LOGIN_RATE_WINDOW_MS),
      String(LOGIN_RATE_MAX_ATTEMPTS),
      String(LOGIN_RATE_BLOCK_MS),
    ],
  });

  const blocked = toInteger(result?.[0], 0) === 1;
  const retryAfterSeconds = Math.max(0, toInteger(result?.[1], 0));
  return {
    blocked,
    retryAfterSeconds,
  };
}

async function clearFailedLoginAttemptsRedis(rateKey) {
  const client = await connectRedisClient();
  if (!client) {
    return null;
  }
  const failKey = buildRateLimitRedisKey("fail", rateKey);
  await client.del(failKey);
  return true;
}

function useMemoryFallback(err = null) {
  resolvedStoreBackend = "memory";
  if (err) {
    logRedisError("Redis operation failed. Using in-memory limiter.", err);
  }
}

async function resolveStoreBackend() {
  if (resolvedStoreBackend) {
    return resolvedStoreBackend;
  }

  if (LOGIN_RATE_STORE_MODE === "memory") {
    resolvedStoreBackend = "memory";
    return resolvedStoreBackend;
  }

  if (!shouldAttemptRedis()) {
    resolvedStoreBackend = "memory";
    return resolvedStoreBackend;
  }

  const client = await connectRedisClient();
  if (client) {
    resolvedStoreBackend = "redis";
    return resolvedStoreBackend;
  }

  resolvedStoreBackend = "memory";
  return resolvedStoreBackend;
}

export async function getRateLimitBlockInfo(rateKey) {
  const backend = await resolveStoreBackend();
  if (backend === "redis") {
    try {
      const redisResult = await getRateLimitBlockInfoRedis(rateKey);
      if (redisResult) {
        return redisResult;
      }
    } catch (err) {
      useMemoryFallback(err);
    }
  }

  return getRateLimitBlockInfoMemory(rateKey);
}

export async function registerFailedLoginAttempt(rateKey) {
  const backend = await resolveStoreBackend();
  if (backend === "redis") {
    try {
      const redisResult = await registerFailedLoginAttemptRedis(rateKey);
      if (redisResult) {
        return redisResult;
      }
    } catch (err) {
      useMemoryFallback(err);
    }
  }

  return registerFailedLoginAttemptMemory(rateKey);
}

export async function clearFailedLoginAttempts(rateKey) {
  const backend = await resolveStoreBackend();
  if (backend === "redis") {
    try {
      const redisResult = await clearFailedLoginAttemptsRedis(rateKey);
      if (redisResult) {
        return;
      }
    } catch (err) {
      useMemoryFallback(err);
    }
  }

  clearFailedLoginAttemptsMemory(rateKey);
}

export async function getLoginRateLimiterBackend() {
  return resolveStoreBackend();
}

async function probeRedisHealth() {
  if (!shouldAttemptRedis()) {
    return {
      configured: false,
      reachable: null,
      reason: "redis_not_configured",
    };
  }

  try {
    const client = await connectRedisClient();
    if (!client) {
      return {
        configured: true,
        reachable: false,
        reason: "connect_failed",
      };
    }

    await client.ping();
    return {
      configured: true,
      reachable: true,
      reason: null,
    };
  } catch (err) {
    useMemoryFallback(err);
    return {
      configured: true,
      reachable: false,
      reason: "ping_failed",
    };
  }
}

export async function getLoginRateLimiterHealth() {
  const backend = await resolveStoreBackend();
  const redisProbe = await probeRedisHealth();

  let redisStatus = "up";
  if (LOGIN_RATE_STORE_MODE === "redis" && redisProbe.reachable !== true) {
    redisStatus = "down";
  } else if (redisProbe.configured && redisProbe.reachable !== true) {
    redisStatus = "degraded";
  }

  return {
    redis: {
      status: redisStatus,
      mode: LOGIN_RATE_STORE_MODE,
      backend,
      configured: redisProbe.configured,
      reachable: redisProbe.reachable,
      reason: redisProbe.reason,
    },
  };
}
