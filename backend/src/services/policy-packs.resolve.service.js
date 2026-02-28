import { query } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { getPolicyPack } from "./policy-packs.service.js";

function toUpper(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function toDbBoolean(value) {
  return value === true || Number(value) === 1;
}

function groupAccountsByCode(rows) {
  const byCode = new Map();
  for (const row of rows || []) {
    const code = String(row.code || "").trim();
    if (!code) {
      continue;
    }
    if (!byCode.has(code)) {
      byCode.set(code, []);
    }
    byCode.get(code).push(row);
  }
  return byCode;
}

function evaluateSuitability(moduleKey, account, rules = {}) {
  const issues = [];
  const normalizedModuleKey = String(moduleKey || "").trim();

  if (!toDbBoolean(account?.is_active)) {
    issues.push("inactive_account");
  }

  const normalizedAccountType = toUpper(account?.account_type);
  const normalizedNormalSide = toUpper(account?.normal_side);
  const allowPosting = toDbBoolean(account?.allow_posting);

  if (rules.accountType && normalizedAccountType !== toUpper(rules.accountType)) {
    issues.push("account_type_mismatch");
  }
  if (rules.normalSide && normalizedNormalSide !== toUpper(rules.normalSide)) {
    issues.push("normal_side_mismatch");
  }

  if (normalizedModuleKey === "cariPosting") {
    if (!allowPosting) {
      issues.push("not_postable");
    }
  } else if (normalizedModuleKey === "shareholderCommitment") {
    if (allowPosting) {
      issues.push("must_be_non_postable");
    }
  } else if (rules.allowPosting !== undefined) {
    const expectedAllowPosting = Boolean(rules.allowPosting);
    if (allowPosting !== expectedAllowPosting) {
      issues.push(
        expectedAllowPosting ? "must_be_postable" : "must_be_non_postable"
      );
    }
  }

  return {
    ok: issues.length === 0,
    issues,
  };
}

function buildResolvedRow({
  moduleKey,
  purposeCode,
  account,
  matchedCode,
}) {
  return {
    moduleKey,
    purposeCode,
    missing: false,
    accountId: parsePositiveInt(account.id),
    accountCode: String(account.code || ""),
    confidence: "HIGH",
    matchedBy: {
      strategy: "codeExact",
      code: String(matchedCode || account.code || ""),
    },
  };
}

function buildMissingRow({
  moduleKey,
  purposeCode,
  reason,
  suggestCreate,
  details = {},
}) {
  return {
    moduleKey,
    purposeCode,
    missing: true,
    reason: String(reason || "no_match"),
    suggestCreate: suggestCreate || null,
    details,
  };
}

function resolvePurposeTarget({
  moduleKey,
  target,
  accountsByCode,
}) {
  const purposeCode = String(target?.purposeCode || "").trim().toUpperCase();
  const codeCandidates = target?.match?.codeExact || [];

  for (const candidateCode of codeCandidates) {
    const normalizedCode = String(candidateCode || "").trim();
    if (!normalizedCode) {
      continue;
    }

    const matches = accountsByCode.get(normalizedCode) || [];
    if (matches.length > 1) {
      return buildMissingRow({
        moduleKey,
        purposeCode,
        reason: "ambiguous_match",
        suggestCreate: target?.suggestCreate || null,
        details: {
          matchedCode: normalizedCode,
          candidateAccountIds: matches
            .map((row) => parsePositiveInt(row.id))
            .filter(Boolean),
        },
      });
    }

    if (matches.length === 1) {
      const account = matches[0];
      const suitability = evaluateSuitability(moduleKey, account, target?.rules);
      if (suitability.ok) {
        return buildResolvedRow({
          moduleKey,
          purposeCode,
          account,
          matchedCode: normalizedCode,
        });
      }

      return buildMissingRow({
        moduleKey,
        purposeCode,
        reason: "unsuitable_match",
        suggestCreate: target?.suggestCreate || null,
        details: {
          matchedCode: normalizedCode,
          accountId: parsePositiveInt(account.id),
          issues: suitability.issues,
        },
      });
    }
  }

  return buildMissingRow({
    moduleKey,
    purposeCode,
    reason: "no_match",
    suggestCreate: target?.suggestCreate || null,
    details: {
      candidateCodesTried: codeCandidates,
    },
  });
}

async function loadLegalEntityAccounts({ tenantId, legalEntityId, runQuery = query }) {
  const result = await runQuery(
    `SELECT
       a.id,
       a.code,
       a.name,
       a.account_type,
       a.normal_side,
       a.is_active,
       a.allow_posting
     FROM accounts a
     JOIN charts_of_accounts c ON c.id = a.coa_id
     WHERE c.tenant_id = ?
       AND c.scope = 'LEGAL_ENTITY'
       AND c.legal_entity_id = ?
     ORDER BY a.id`,
    [tenantId, legalEntityId]
  );

  return result.rows || [];
}

export async function resolvePolicyPack({
  tenantId,
  legalEntityId,
  packId,
  runQuery = query,
}) {
  const normalizedTenantId = parsePositiveInt(tenantId);
  const normalizedLegalEntityId = parsePositiveInt(legalEntityId);
  if (!normalizedTenantId) {
    throw badRequest("tenantId is required");
  }
  if (!normalizedLegalEntityId) {
    throw badRequest("legalEntityId is required");
  }

  const pack = getPolicyPack(packId);
  if (!pack) {
    return null;
  }

  const accounts = await loadLegalEntityAccounts({
    tenantId: normalizedTenantId,
    legalEntityId: normalizedLegalEntityId,
    runQuery,
  });
  const accountsByCode = groupAccountsByCode(accounts);

  const rows = [];
  for (const module of pack.modules || []) {
    const moduleKey = String(module?.moduleKey || "").trim();
    for (const target of module?.purposeTargets || []) {
      rows.push(
        resolvePurposeTarget({
          moduleKey,
          target,
          accountsByCode,
        })
      );
    }
  }

  const missingCount = rows.filter((row) => row.missing).length;
  return {
    packId: pack.packId,
    legalEntityId: normalizedLegalEntityId,
    rows,
    summary: {
      total: rows.length,
      resolved: rows.length - missingCount,
      missing: missingCount,
    },
  };
}

