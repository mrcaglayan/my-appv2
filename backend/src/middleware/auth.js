import jwt from "jsonwebtoken";
import { getAuthCookieName, readCookieValue } from "../auth/cookieSession.js";

export function requireAuth(req, res, next) {
  const token = readCookieValue(req, getAuthCookieName());
  const jwtSecret = process.env.JWT_SECRET;

  if (!token) {
    return res.status(401).json({ message: "Missing authentication cookie" });
  }

  if (!jwtSecret) {
    return res.status(500).json({ message: "JWT secret is not configured" });
  }

  try {
    const payload = jwt.verify(token, jwtSecret);
    req.user = payload;
    return next();
  } catch {
    return res.status(401).json({ message: "Invalid or expired token" });
  }
}
