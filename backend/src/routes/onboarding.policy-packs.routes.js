import express from "express";
import { requirePermission } from "../middleware/rbac.js";
import { asyncHandler } from "./_utils.js";
import {
  getPolicyPack,
  listPolicyPacks,
} from "../services/policy-packs.service.js";

const router = express.Router();

router.get(
  "/policy-packs",
  requirePermission("org.tree.read"),
  asyncHandler(async (req, res) => {
    return res.json({
      rows: listPolicyPacks(),
    });
  })
);

router.get(
  "/policy-packs/:packId",
  requirePermission("org.tree.read"),
  asyncHandler(async (req, res) => {
    const row = getPolicyPack(req.params?.packId);
    if (!row) {
      const err = new Error("Policy pack not found");
      err.status = 404;
      throw err;
    }

    return res.json({ row });
  })
);

export default router;

