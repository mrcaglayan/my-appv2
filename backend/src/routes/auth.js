import express from "express";
import bcrypt from "bcrypt";
import jwt from "jsonwebtoken";
import { query } from "../db.js";
import {
  clearFailedLoginAttempts,
  getRateLimitBlockInfo,
  registerFailedLoginAttempt,
} from "../auth/loginRateLimiter.js";
import {
  getAuthCookieClearOptions,
  getAuthCookieName,
  getAuthCookieOptions,
} from "../auth/cookieSession.js";

const router = express.Router();
const AUTH_TOKEN_EXPIRES_IN = String(process.env.AUTH_TOKEN_EXPIRES_IN || "7d");

function resolveClientIp(req) {
  const forwardedFor = String(req.headers["x-forwarded-for"] || "").trim();
  if (forwardedFor) {
    const firstIp = forwardedFor
      .split(",")
      .map((segment) => segment.trim())
      .find(Boolean);
    if (firstIp) {
      return firstIp.slice(0, 64);
    }
  }
  return String(req.ip || req.socket?.remoteAddress || "unknown").slice(0, 64);
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function sendRateLimitResponse(res, retryAfterSeconds) {
  res.set("Retry-After", String(retryAfterSeconds));
  return res.status(429).json({
    message: "Too many login attempts. Try again later.",
    retryAfterSeconds,
  });
}

// POST /auth/login
router.post("/login", async (req, res, next) => {
  try {
    const { email, password } = req.body || {};

    if (!email || !password) {
      return res.status(400).json({ message: "Email and password required" });
    }

    const normalizedEmail = normalizeEmail(email);
    const loginEmail = String(email).trim();
    const clientIp = resolveClientIp(req);
    const rateKey = `${clientIp}|${normalizedEmail}`;
    const blockInfo = await getRateLimitBlockInfo(rateKey);
    if (blockInfo.blocked) {
      return sendRateLimitResponse(res, blockInfo.retryAfterSeconds);
    }

    const { rows } = await query(
      `SELECT id, email, password_hash, name, tenant_id, status
       FROM users
       WHERE email = ?`,
      [loginEmail]
    );

    const user = rows[0];
    if (!user) {
      const failedAttempt = await registerFailedLoginAttempt(rateKey);
      if (failedAttempt.blocked) {
        return sendRateLimitResponse(res, failedAttempt.retryAfterSeconds);
      }
      return res.status(401).json({ message: "Invalid credentials" });
    }

    const ok = await bcrypt.compare(password, user.password_hash);
    if (!ok || String(user.status || "").toUpperCase() !== "ACTIVE") {
      const failedAttempt = await registerFailedLoginAttempt(rateKey);
      if (failedAttempt.blocked) {
        return sendRateLimitResponse(res, failedAttempt.retryAfterSeconds);
      }
      return res.status(401).json({ message: "Invalid credentials" });
    }

    await clearFailedLoginAttempts(rateKey);

    if (!process.env.JWT_SECRET) {
      return res.status(500).json({ message: "JWT secret is not configured" });
    }

    const token = jwt.sign(
      { userId: user.id, email: user.email, tenantId: user.tenant_id || null },
      process.env.JWT_SECRET,
      { expiresIn: AUTH_TOKEN_EXPIRES_IN }
    );

    res.cookie(getAuthCookieName(), token, getAuthCookieOptions());
    return res.json({
      ok: true,
      expiresIn: AUTH_TOKEN_EXPIRES_IN,
    });
  } catch (err) {
    return next(err);
  }
});

// POST /auth/logout
router.post("/logout", (req, res) => {
  res.clearCookie(getAuthCookieName(), getAuthCookieClearOptions());
  return res.json({ ok: true });
});

export default router;
