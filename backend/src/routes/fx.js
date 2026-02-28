import express from "express";
import { query } from "../db.js";
import { requirePermission } from "../middleware/rbac.js";
import {
  asyncHandler,
  badRequest,
  resolveTenantId,
} from "./_utils.js";

const router = express.Router();

router.post(
  "/rates/bulk-upsert",
  requirePermission("fx.rate.bulk_upsert"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const rates = Array.isArray(req.body?.rates) ? req.body.rates : [];
    if (rates.length === 0) {
      throw badRequest("rates must be a non-empty array");
    }

    for (const rate of rates) {
      const { rateDate, fromCurrencyCode, toCurrencyCode, rateType, value, source } =
        rate || {};
      if (
        !rateDate ||
        !fromCurrencyCode ||
        !toCurrencyCode ||
        !rateType ||
        value === undefined ||
        value === null
      ) {
        throw badRequest(
          "Each rate item requires rateDate, fromCurrencyCode, toCurrencyCode, rateType, value"
        );
      }

      await query(
        `INSERT INTO fx_rates (
            tenant_id, rate_date, from_currency_code, to_currency_code, rate_type, rate, source
          )
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           rate = VALUES(rate),
           source = VALUES(source)`,
        [
          tenantId,
          String(rateDate),
          String(fromCurrencyCode).toUpperCase(),
          String(toCurrencyCode).toUpperCase(),
          String(rateType).toUpperCase(),
          Number(value),
          source ? String(source) : null,
        ]
      );
    }

    return res.status(201).json({
      ok: true,
      tenantId,
      upserted: rates.length,
    });
  })
);

router.get(
  "/rates",
  requirePermission("fx.rate.read"),
  asyncHandler(async (req, res) => {
    const tenantId = resolveTenantId(req);
    if (!tenantId) {
      throw badRequest("tenantId is required");
    }

    const dateFrom = req.query.dateFrom || "1900-01-01";
    const dateTo = req.query.dateTo || "2999-12-31";
    const fromCurrencyCode = req.query.fromCurrencyCode
      ? String(req.query.fromCurrencyCode).toUpperCase()
      : null;
    const toCurrencyCode = req.query.toCurrencyCode
      ? String(req.query.toCurrencyCode).toUpperCase()
      : null;
    const rateType = req.query.rateType
      ? String(req.query.rateType).toUpperCase()
      : null;

    const conditions = ["tenant_id = ?", "rate_date BETWEEN ? AND ?"];
    const params = [tenantId, dateFrom, dateTo];

    if (fromCurrencyCode) {
      conditions.push("from_currency_code = ?");
      params.push(fromCurrencyCode);
    }
    if (toCurrencyCode) {
      conditions.push("to_currency_code = ?");
      params.push(toCurrencyCode);
    }
    if (rateType) {
      conditions.push("rate_type = ?");
      params.push(rateType);
    }

    const result = await query(
      `SELECT id, rate_date, from_currency_code, to_currency_code, rate_type, rate, source, is_locked
       FROM fx_rates
       WHERE ${conditions.join(" AND ")}
       ORDER BY rate_date DESC, from_currency_code, to_currency_code, rate_type`,
      params
    );

    return res.json({
      tenantId,
      rows: result.rows,
    });
  })
);

export default router;
