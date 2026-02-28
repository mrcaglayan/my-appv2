import { query } from "../db.js";
import { assertScopeAccess, requirePermission } from "../middleware/rbac.js";
import {
  assertAccountBelongsToTenant,
  assertCoaBelongsToTenant,
  assertFiscalCalendarBelongsToTenant,
  assertLegalEntityBelongsToTenant,
} from "../tenantGuards.js";
import {
  asyncHandler,
  assertRequiredFields,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";

export function registerGlWriteCoreRoutes(router) {
  router.post(
    "/books",
    requirePermission("gl.book.upsert", {
      resolveScope: (req, tenantId) => {
        const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
        return legalEntityId
          ? { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId }
          : { scopeType: "TENANT", scopeId: tenantId };
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) throw badRequest("tenantId is required");

      assertRequiredFields(req.body, [
        "legalEntityId",
        "calendarId",
        "code",
        "name",
        "baseCurrencyCode",
      ]);

      const legalEntityId = parsePositiveInt(req.body.legalEntityId);
      const calendarId = parsePositiveInt(req.body.calendarId);
      if (!legalEntityId || !calendarId) {
        throw badRequest("legalEntityId and calendarId must be positive integers");
      }

      await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
      await assertFiscalCalendarBelongsToTenant(tenantId, calendarId, "calendarId");
      assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");

      const { code, name, bookType = "LOCAL", baseCurrencyCode } = req.body;
      const result = await query(
        `INSERT INTO books (
            tenant_id, legal_entity_id, calendar_id, code, name, book_type, base_currency_code
          )
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           legal_entity_id = VALUES(legal_entity_id),
           calendar_id = VALUES(calendar_id),
           name = VALUES(name),
           book_type = VALUES(book_type),
           base_currency_code = VALUES(base_currency_code)`,
        [
          tenantId,
          legalEntityId,
          calendarId,
          String(code).trim(),
          String(name).trim(),
          String(bookType).toUpperCase(),
          String(baseCurrencyCode).toUpperCase(),
        ]
      );

      return res.status(201).json({ ok: true, id: result.rows.insertId || null });
    })
  );

  router.post(
    "/coas",
    requirePermission("gl.coa.upsert", {
      resolveScope: (req, tenantId) => {
        const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
        return legalEntityId
          ? { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId }
          : { scopeType: "TENANT", scopeId: tenantId };
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) throw badRequest("tenantId is required");

      assertRequiredFields(req.body, ["scope", "code", "name"]);
      const scope = String(req.body.scope || "").toUpperCase();
      if (!["LEGAL_ENTITY", "GROUP"].includes(scope)) {
        throw badRequest("scope must be LEGAL_ENTITY or GROUP");
      }

      const legalEntityId = req.body.legalEntityId
        ? parsePositiveInt(req.body.legalEntityId)
        : null;
      if (scope === "LEGAL_ENTITY" && !legalEntityId) {
        throw badRequest("legalEntityId is required for LEGAL_ENTITY scope");
      }
      if (scope === "GROUP" && legalEntityId) {
        throw badRequest("legalEntityId must be omitted for GROUP scope");
      }
      if (legalEntityId) {
        await assertLegalEntityBelongsToTenant(tenantId, legalEntityId, "legalEntityId");
        assertScopeAccess(req, "legal_entity", legalEntityId, "legalEntityId");
      }

      const { code, name } = req.body;
      const result = await query(
        `INSERT INTO charts_of_accounts (tenant_id, legal_entity_id, scope, code, name)
         VALUES (?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           name = VALUES(name),
           scope = VALUES(scope),
           legal_entity_id = VALUES(legal_entity_id)`,
        [tenantId, legalEntityId, scope, String(code).trim(), String(name).trim()]
      );

      return res.status(201).json({ ok: true, id: result.rows.insertId || null });
    })
  );

  router.post(
    "/accounts",
    requirePermission("gl.account.upsert", {
      resolveScope: async (req, tenantId) => {
        const coaId = parsePositiveInt(req.body?.coaId);
        if (!coaId) {
          return { scopeType: "TENANT", scopeId: tenantId };
        }

        const coaResult = await query(
          `SELECT legal_entity_id
           FROM charts_of_accounts
           WHERE id = ?
             AND tenant_id = ?
           LIMIT 1`,
          [coaId, tenantId]
        );
        const legalEntityId = parsePositiveInt(coaResult.rows[0]?.legal_entity_id);
        if (legalEntityId) {
          return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
        }
        return { scopeType: "TENANT", scopeId: tenantId };
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) throw badRequest("tenantId is required");

      assertRequiredFields(req.body, [
        "coaId",
        "code",
        "name",
        "accountType",
        "normalSide",
      ]);

      const coaId = parsePositiveInt(req.body.coaId);
      if (!coaId) throw badRequest("coaId must be a positive integer");

      const coa = await assertCoaBelongsToTenant(tenantId, coaId, "coaId");
      const coaLegalEntityId = parsePositiveInt(coa.legal_entity_id);
      if (coaLegalEntityId) {
        assertScopeAccess(req, "legal_entity", coaLegalEntityId, "coaId");
      }

      const parentAccountId = req.body.parentAccountId
        ? parsePositiveInt(req.body.parentAccountId)
        : null;
      if (req.body.parentAccountId && !parentAccountId) {
        throw badRequest("parentAccountId must be a positive integer");
      }
      if (parentAccountId) {
        const parent = await assertAccountBelongsToTenant(
          tenantId,
          parentAccountId,
          "parentAccountId"
        );
        if (parsePositiveInt(parent.coa_id) !== coaId) {
          throw badRequest("parentAccountId must belong to the same coaId");
        }
      }

      const { code, name, accountType, normalSide, allowPosting = true } = req.body;
      const normalizedCode = String(code).trim();
      const normalizedName = String(name).trim();
      const normalizedAccountType = String(accountType).toUpperCase();
      const normalizedNormalSide = String(normalSide).toUpperCase();
      const requestedAllowPosting = Boolean(allowPosting);

      await query(
        `INSERT INTO accounts (
            coa_id, code, name, account_type, normal_side, allow_posting, parent_account_id
          )
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           name = VALUES(name),
           account_type = VALUES(account_type),
           normal_side = VALUES(normal_side),
           allow_posting = VALUES(allow_posting),
           parent_account_id = VALUES(parent_account_id)`,
        [
          coaId,
          normalizedCode,
          normalizedName,
          normalizedAccountType,
          normalizedNormalSide,
          requestedAllowPosting,
          parentAccountId,
        ]
      );

      const savedResult = await query(
        `SELECT
           a.id,
           a.allow_posting,
           EXISTS(
             SELECT 1
             FROM accounts child
             WHERE child.parent_account_id = a.id
           ) AS has_children
         FROM accounts a
         WHERE a.coa_id = ?
           AND a.code = ?
         LIMIT 1`,
        [coaId, normalizedCode]
      );
      const saved = savedResult.rows[0];
      const savedAccountId = parsePositiveInt(saved?.id);
      if (!savedAccountId) {
        throw badRequest("Failed to resolve account after upsert");
      }

      if (parentAccountId) {
        await query(
          `UPDATE accounts
           SET allow_posting = FALSE
           WHERE id = ?
             AND allow_posting = TRUE`,
          [parentAccountId]
        );
      }

      if (Boolean(saved?.has_children)) {
        await query(
          `UPDATE accounts
           SET allow_posting = FALSE
           WHERE id = ?
             AND allow_posting = TRUE`,
          [savedAccountId]
        );
      }

      const effectiveAllowPosting =
        Boolean(saved?.allow_posting) && !Boolean(saved?.has_children);

      return res.status(201).json({
        ok: true,
        id: savedAccountId,
        allowPosting: effectiveAllowPosting,
        enforcedNonPosting:
          Boolean(saved?.has_children) &&
          requestedAllowPosting &&
          !effectiveAllowPosting,
      });
    })
  );

  router.post(
    "/account-mappings",
    requirePermission("gl.account_mapping.upsert", {
      resolveScope: async (req, tenantId) => {
        const sourceAccountId = parsePositiveInt(req.body?.sourceAccountId);
        if (!sourceAccountId) {
          return { scopeType: "TENANT", scopeId: tenantId };
        }

        const sourceResult = await query(
          `SELECT c.legal_entity_id
           FROM accounts a
           JOIN charts_of_accounts c ON c.id = a.coa_id
           WHERE a.id = ?
             AND c.tenant_id = ?
           LIMIT 1`,
          [sourceAccountId, tenantId]
        );

        const legalEntityId = parsePositiveInt(sourceResult.rows[0]?.legal_entity_id);
        if (legalEntityId) {
          return { scopeType: "LEGAL_ENTITY", scopeId: legalEntityId };
        }
        return { scopeType: "TENANT", scopeId: tenantId };
      },
    }),
    asyncHandler(async (req, res) => {
      const tenantId = resolveTenantId(req);
      if (!tenantId) throw badRequest("tenantId is required");

      assertRequiredFields(req.body, ["sourceAccountId", "targetAccountId"]);
      const sourceAccountId = parsePositiveInt(req.body.sourceAccountId);
      const targetAccountId = parsePositiveInt(req.body.targetAccountId);
      if (!sourceAccountId || !targetAccountId) {
        throw badRequest("sourceAccountId and targetAccountId must be positive integers");
      }

      const sourceAccount = await assertAccountBelongsToTenant(
        tenantId,
        sourceAccountId,
        "sourceAccountId"
      );
      const targetAccount = await assertAccountBelongsToTenant(
        tenantId,
        targetAccountId,
        "targetAccountId"
      );

      const sourceEntityId = parsePositiveInt(sourceAccount.legal_entity_id);
      const targetEntityId = parsePositiveInt(targetAccount.legal_entity_id);
      if (sourceEntityId) {
        assertScopeAccess(req, "legal_entity", sourceEntityId, "sourceAccount.legalEntityId");
      }
      if (targetEntityId) {
        assertScopeAccess(req, "legal_entity", targetEntityId, "targetAccount.legalEntityId");
      }

      const mappingType = String(req.body.mappingType || "LOCAL_TO_GROUP").toUpperCase();
      const result = await query(
        `INSERT INTO account_mappings (
            tenant_id, source_account_id, target_account_id, mapping_type
          )
         VALUES (?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           mapping_type = VALUES(mapping_type)`,
        [tenantId, sourceAccountId, targetAccountId, mappingType]
      );

      return res.status(201).json({ ok: true, id: result.rows.insertId || null });
    })
  );
}
