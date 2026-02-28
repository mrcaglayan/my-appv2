import express from "express";
import { asyncHandler } from "./_utils.js";

const CASH_CONTROL_MODES = new Set(["OFF", "WARN", "ENFORCE"]);

function normalizeCashControlMode(value) {
  const normalized = String(value || "ENFORCE").trim().toUpperCase();
  if (CASH_CONTROL_MODES.has(normalized)) {
    return normalized;
  }
  return "ENFORCE";
}

const router = express.Router();

router.get(
  "/",
  asyncHandler(async (req, res) => {
    const cashControlMode = normalizeCashControlMode(process.env.GL_CASH_CONTROL_MODE);
    return res.json({
      cashControlMode,
      requestId: req.requestId || null,
    });
  })
);

export default router;
