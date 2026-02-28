import { query } from "../db.js";
import { badRequest, parsePositiveInt } from "../routes/_utils.js";
import { listActiveRulesForAutomation } from "./bank.reconciliationRules.service.js";
import { upsertReconciliationException } from "./bank.reconciliationExceptions.service.js";
import { matchReconciliationLine } from "./bank.reconciliation.service.js";
import { autoPostTemplateAndReconcileStatementLine } from "./bank.reconciliationAutoPosting.service.js";
import {
  findPaymentLineCandidatesForReturnAutomation,
  processPaymentReturnFromStatementLine,
} from "./bank.paymentReturns.service.js";
import {
  autoMatchPaymentLineWithDifferenceAndReconcile,
  findPaymentLineCandidatesForDifferenceAutomation,
} from "./bank.reconciliationDifferences.service.js";
import { resolveBankAccountScope } from "./bank.accounts.service.js";

const AMOUNT_EPSILON = 0.01;

function u(value) {
  return String(value || "").trim().toUpperCase();
}

function toAmount(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Number(n.toFixed(6)) : 0;
}

function absAmount(value) {
  return Math.abs(toAmount(value));
}

function safeJson(value) {
  return JSON.stringify(value ?? null);
}

function parseJson(value, fallback = null) {
  if (value === undefined || value === null) return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function arr(v) {
  if (!Array.isArray(v)) return [];
  return v.map((x) => u(x)).filter(Boolean).slice(0, 50);
}

function b(v, fallback = false) {
  if (v === undefined || v === null) return fallback;
  if (typeof v === "boolean") return v;
  const n = String(v).trim().toLowerCase();
  if (["1", "true", "yes", "y"].includes(n)) return true;
  if (["0", "false", "no", "n"].includes(n)) return false;
  return fallback;
}

function amountTolerance(rule) {
  const raw = Number(rule?.conditions_json?.amountTolerance ?? rule?.conditions_json?.amount_tolerance);
  if (!Number.isFinite(raw) || raw < 0) return AMOUNT_EPSILON;
  return Number(raw.toFixed(6));
}

function dateLagDays(rule) {
  const raw = Number(
    rule?.conditions_json?.dateLagDays ??
      rule?.conditions_json?.date_lag_days ??
      rule?.conditions_json?.dateWindowDays ??
      7
  );
  if (!Number.isInteger(raw) || raw < 0 || raw > 365) return 7;
  return raw;
}

function tokenize(...values) {
  const set = new Set();
  for (const value of values) {
    const text = u(value);
    if (!text) continue;
    for (const token of text.split(/[\s,.;:/\\|()[\]-]+/g)) {
      if (token && token.length >= 3) set.add(token);
    }
  }
  return Array.from(set).slice(0, 20);
}

function remainingAmountAbs(line) {
  const explicit = Number(line?.remaining_amount_abs);
  if (Number.isFinite(explicit) && explicit >= 0) return Number(explicit.toFixed(6));
  return Number(Math.max(0, absAmount(line?.amount) - absAmount(line?.active_matched_total)).toFixed(6));
}

function ruleTrace(rule) {
  if (!rule) return null;
  return {
    ruleId: parsePositiveInt(rule.id),
    ruleCode: rule.rule_code || null,
    ruleName: rule.rule_name || null,
    priority: Number(rule.priority || 0),
    matchType: rule.match_type || null,
    actionType: rule.action_type || null,
  };
}

function targetSummary(t) {
  if (!t) return null;
  return {
    entityType: t.entityType,
    entityId: t.entityId,
    paymentBatchId: t.paymentBatchId || null,
    differenceProfileId: t.differenceProfileId || null,
    differenceAmountAbs:
      t.differenceAmountAbs === null || t.differenceAmountAbs === undefined
        ? null
        : toAmount(t.differenceAmountAbs),
    amount: toAmount(t.amount),
    displayRef: t.displayRef || null,
    displayText: t.displayText || null,
  };
}

function getAutoPostTemplateTargetFromRule(rule, line) {
  const payload = rule?.action_payload_json || {};
  const templateId = parsePositiveInt(
    payload.postingTemplateId ??
      payload.posting_template_id ??
      payload.templateId ??
      payload.template_id
  );
  if (!templateId) return null;
  return {
    entityType: "POSTING_TEMPLATE",
    entityId: templateId,
    amount: remainingAmountAbs(line),
    displayRef: payload.templateCode || payload.template_code || `B08TPL#${templateId}`,
    displayText: payload.templateName || payload.template_name || "Auto-post template",
    date: line?.txn_date || null,
  };
}

function getDifferenceProfileIdFromRule(rule) {
  const payload = rule?.action_payload_json || {};
  return parsePositiveInt(
    payload.differenceProfileId ??
      payload.difference_profile_id ??
      payload.profileId ??
      payload.profile_id
  );
}

function confidenceScore({ rule, line, target, exactRef = false }) {
  let score = 65;
  if (u(rule?.match_type).includes("REFERENCE")) score += 10;
  if (exactRef) score += 15;
  if (Math.abs(absAmount(target?.amount) - remainingAmountAbs(line)) <= AMOUNT_EPSILON) score += 10;
  if (String(line?.txn_date || "") && String(target?.date || "") === String(line.txn_date || "")) score += 5;
  return Math.min(100, Math.max(0, score));
}

function lineDirection(line) {
  const n = Number(line?.amount || 0);
  if (!Number.isFinite(n) || n === 0) return "ZERO";
  return n > 0 ? "IN" : "OUT";
}

function policyCheck(line, rule) {
  const c = rule?.conditions_json || {};
  const dir = u(c.debitCredit ?? c.debit_credit);
  if (dir && ["IN", "OUT"].includes(dir)) {
    const actual = lineDirection(line);
    if (actual !== "ZERO" && actual !== dir) {
      return { blocked: true, reasonCode: "POLICY_BLOCKED", reasonMessage: `Direction ${actual} blocked by ${dir}` };
    }
  }
  const reqCurrency = u(c.currencyCode ?? c.currency_code);
  if (reqCurrency && reqCurrency !== u(line.currency_code)) {
    return { blocked: true, reasonCode: "POLICY_BLOCKED", reasonMessage: `Currency ${line.currency_code} blocked by ${reqCurrency}` };
  }
  return { blocked: false };
}

function textPrecheck(line, rule) {
  const c = rule?.conditions_json || {};
  const refNeedles = arr(c.referenceIncludesAny ?? c.reference_includes_any);
  const textNeedles = arr(c.textIncludesAny ?? c.text_includes_any);
  const ref = u(line.reference_no);
  const text = `${ref} ${u(line.description)}`.trim();
  if (refNeedles.length && !refNeedles.some((n) => ref.includes(n))) return false;
  if (textNeedles.length && !textNeedles.some((n) => text.includes(n))) return false;
  return true;
}

function applicable(rule, line) {
  const scope = u(rule.scope_type);
  if (scope === "LEGAL_ENTITY") {
    return parsePositiveInt(rule.legal_entity_id) === parsePositiveInt(line.legal_entity_id);
  }
  if (scope === "BANK_ACCOUNT") {
    return (
      parsePositiveInt(rule.legal_entity_id) === parsePositiveInt(line.legal_entity_id) &&
      parsePositiveInt(rule.bank_account_id) === parsePositiveInt(line.bank_account_id)
    );
  }
  return true;
}

function effective(rule, line) {
  const d = String(line?.txn_date || "").slice(0, 10);
  const from = String(rule?.effective_from || "").slice(0, 10);
  const to = String(rule?.effective_to || "").slice(0, 10);
  if (from && d && d < from) return false;
  if (to && d && d > to) return false;
  return true;
}

async function loadLines({ req, tenantId, filters, buildScopeFilter, assertScopeAccess }) {
  const params = [tenantId];
  const where = ["l.tenant_id = ?"];
  where.push(buildScopeFilter(req, "legal_entity", "l.legal_entity_id", params));
  where.push("l.recon_status IN ('UNMATCHED','PARTIAL')");

  if (filters.legalEntityId) {
    assertScopeAccess(req, "legal_entity", filters.legalEntityId, "legalEntityId");
    where.push("l.legal_entity_id = ?");
    params.push(filters.legalEntityId);
  }
  if (filters.bankAccountId) {
    const scope = await resolveBankAccountScope(filters.bankAccountId, tenantId);
    if (!scope) throw badRequest("bankAccountId not found");
    assertScopeAccess(req, "legal_entity", scope.scopeId, "bankAccountId");
    if (filters.legalEntityId && parsePositiveInt(filters.legalEntityId) !== parsePositiveInt(scope.scopeId)) {
      throw badRequest("bankAccountId does not belong to legalEntityId");
    }
    where.push("l.bank_account_id = ?");
    params.push(filters.bankAccountId);
  }
  if (filters.dateFrom) {
    where.push("l.txn_date >= ?");
    params.push(filters.dateFrom);
  }
  if (filters.dateTo) {
    where.push("l.txn_date <= ?");
    params.push(filters.dateTo);
  }

  const safeLimit = Number.isInteger(filters.limit) && filters.limit > 0 ? filters.limit : 100;
  const result = await query(
    `SELECT
        l.id, l.tenant_id, l.legal_entity_id, l.bank_account_id, l.txn_date, l.value_date,
        l.description, l.reference_no, l.amount, l.currency_code, l.recon_status,
        l.reconciliation_method, l.reconciliation_rule_id, l.reconciliation_confidence,
        ba.code AS bank_account_code, ba.name AS bank_account_name, ba.gl_account_id AS bank_gl_account_id,
        COALESCE(m.active_matched_total, 0) AS active_matched_total
     FROM bank_statement_lines l
     JOIN bank_accounts ba
       ON ba.tenant_id = l.tenant_id AND ba.legal_entity_id = l.legal_entity_id AND ba.id = l.bank_account_id
     LEFT JOIN (
       SELECT tenant_id, statement_line_id, COALESCE(SUM(matched_amount),0) AS active_matched_total
       FROM bank_reconciliation_matches
       WHERE tenant_id = ? AND status = 'ACTIVE'
       GROUP BY tenant_id, statement_line_id
     ) m ON m.tenant_id = l.tenant_id AND m.statement_line_id = l.id
     WHERE ${where.join(" AND ")}
     ORDER BY l.txn_date ASC, l.id ASC
     LIMIT ${safeLimit}`,
    [tenantId, ...params]
  );
  return (result.rows || []).map((r) => ({ ...r, active_matched_total: toAmount(r.active_matched_total), remaining_amount_abs: remainingAmountAbs(r) }));
}

async function paymentBatchCandidates({ tenantId, line, rule }) {
  const rem = remainingAmountAbs(line);
  if (!(rem > 0)) return [];
  const lag = dateLagDays(rule);
  const result = await query(
    `SELECT
        pb.id AS batch_id,
        pb.batch_no,
        DATE(pb.posted_at) AS posted_date,
        COALESCE(SUM(CASE
          WHEN pl.executed_amount > 0 THEN ABS(pl.executed_amount)
          WHEN pl.exported_amount > 0 THEN ABS(pl.exported_amount)
          ELSE ABS(pl.amount)
        END),0) AS total_amount,
        GROUP_CONCAT(DISTINCT CONCAT_WS(' ', pb.batch_no, pl.bank_reference, pl.external_payment_ref, pl.beneficiary_bank_ref, pl.payable_ref, pl.beneficiary_name) SEPARATOR ' || ') AS blob
     FROM payment_batches pb
     JOIN payment_batch_lines pl
       ON pl.tenant_id = pb.tenant_id AND pl.legal_entity_id = pb.legal_entity_id AND pl.batch_id = pb.id
     WHERE pb.tenant_id = ?
       AND pb.legal_entity_id = ?
       AND pb.bank_account_id = ?
       AND pb.status = 'POSTED'
       AND (pb.posted_at IS NULL OR DATE(pb.posted_at) BETWEEN DATE_SUB(?, INTERVAL ${lag} DAY) AND DATE_ADD(?, INTERVAL ${lag} DAY))
     GROUP BY pb.id, pb.batch_no, DATE(pb.posted_at)
     ORDER BY pb.id DESC
     LIMIT 200`,
    [tenantId, line.legal_entity_id, line.bank_account_id, line.txn_date, line.txn_date]
  );

  const tol = amountTolerance(rule);
  const ref = u(line.reference_no);
  const desc = u(line.description);
  const toks = tokenize(line.reference_no, line.description);
  const refNeedles = arr(rule?.conditions_json?.referenceIncludesAny ?? rule?.conditions_json?.reference_includes_any);
  const textNeedles = arr(rule?.conditions_json?.textIncludesAny ?? rule?.conditions_json?.text_includes_any);
  const requireRef = b(rule?.conditions_json?.requireReference ?? rule?.conditions_json?.require_reference, false);

  const rows = [];
  for (const r of result.rows || []) {
    const total = absAmount(r.total_amount);
    if (Math.abs(total - rem) > tol) continue;
    const blob = u(r.blob);
    if (requireRef && ref && !blob.includes(ref)) continue;
    if (refNeedles.length && !refNeedles.some((n) => blob.includes(n))) continue;
    if (textNeedles.length && !textNeedles.some((n) => blob.includes(n))) continue;
    if (!ref && !desc && refNeedles.length === 0 && textNeedles.length === 0) continue;
    const exactRef = ref ? blob.includes(ref) : false;
    const tokenHit = toks.length ? toks.some((t) => blob.includes(t)) : false;
    if (!exactRef && !tokenHit && refNeedles.length === 0 && textNeedles.length === 0) continue;
    const target = {
      entityType: "PAYMENT_BATCH",
      entityId: parsePositiveInt(r.batch_id),
      amount: total,
      displayRef: r.batch_no || `PB#${r.batch_id}`,
      displayText: null,
      date: r.posted_date || null,
    };
    rows.push({ ...target, confidence: confidenceScore({ rule, line, target, exactRef }) });
  }
  return rows.sort((a, b) => b.confidence - a.confidence || b.entityId - a.entityId);
}

async function journalCandidates({ tenantId, line, rule }) {
  const rem = remainingAmountAbs(line);
  if (!(rem > 0) || !parsePositiveInt(line.bank_gl_account_id)) return [];
  const lag = dateLagDays(rule);
  const result = await query(
    `SELECT
        je.id AS journal_id,
        je.journal_no,
        je.entry_date,
        je.reference_no,
        je.description,
        ABS(COALESCE(SUM(jl.debit_base - jl.credit_base),0)) AS bank_gl_amount
     FROM journal_entries je
     JOIN journal_lines jl ON jl.journal_entry_id = je.id
     WHERE je.tenant_id = ?
       AND je.legal_entity_id = ?
       AND je.status = 'POSTED'
       AND jl.account_id = ?
       AND je.entry_date BETWEEN DATE_SUB(?, INTERVAL ${lag} DAY) AND DATE_ADD(?, INTERVAL ${lag} DAY)
     GROUP BY je.id, je.journal_no, je.entry_date, je.reference_no, je.description
     ORDER BY je.entry_date DESC, je.id DESC
     LIMIT 200`,
    [tenantId, line.legal_entity_id, line.bank_gl_account_id, line.txn_date, line.txn_date]
  );

  const tol = amountTolerance(rule);
  const ref = u(line.reference_no);
  const toks = tokenize(line.reference_no, line.description);
  const refNeedles = arr(rule?.conditions_json?.referenceIncludesAny ?? rule?.conditions_json?.reference_includes_any);
  const textNeedles = arr(rule?.conditions_json?.textIncludesAny ?? rule?.conditions_json?.text_includes_any);
  const requireExactRef = u(rule.match_type) === "JOURNAL_BY_REFERENCE_AND_AMOUNT";
  const rows = [];
  for (const r of result.rows || []) {
    const total = absAmount(r.bank_gl_amount);
    if (Math.abs(total - rem) > tol) continue;
    const blob = u(`${r.journal_no || ""} ${r.reference_no || ""} ${r.description || ""}`);
    if (refNeedles.length && !refNeedles.some((n) => blob.includes(n))) continue;
    if (textNeedles.length && !textNeedles.some((n) => blob.includes(n))) continue;
    const exactRef = ref ? blob.includes(ref) : false;
    const tokenHit = toks.length ? toks.some((t) => blob.includes(t)) : false;
    if (requireExactRef && !exactRef) continue;
    if (!requireExactRef && refNeedles.length === 0 && textNeedles.length === 0 && !exactRef && !tokenHit) continue;
    const target = {
      entityType: "JOURNAL",
      entityId: parsePositiveInt(r.journal_id),
      amount: total,
      displayRef: r.journal_no || `JE#${r.journal_id}`,
      displayText: r.description || r.reference_no || null,
      date: r.entry_date || null,
    };
    rows.push({ ...target, confidence: confidenceScore({ rule, line, target, exactRef }) });
  }
  return rows.sort((a, b) => b.confidence - a.confidence || b.entityId - a.entityId);
}

async function candidatesForRule({ tenantId, line, rule }) {
  const mt = u(rule.match_type);
  const action = u(rule.action_type);
  if (action === "PROCESS_PAYMENT_RETURN") {
    return findPaymentLineCandidatesForReturnAutomation({ tenantId, line, rule });
  }
  if (action === "AUTO_MATCH_PAYMENT_LINE_WITH_DIFFERENCE") {
    return findPaymentLineCandidatesForDifferenceAutomation({ tenantId, line, rule });
  }
  if (mt === "PAYMENT_BY_BANK_REFERENCE" || mt === "PAYMENT_BY_TEXT_AND_AMOUNT") {
    return paymentBatchCandidates({ tenantId, line, rule });
  }
  if (mt === "JOURNAL_BY_TEXT_AND_AMOUNT" || mt === "JOURNAL_BY_REFERENCE_AND_AMOUNT") {
    return journalCandidates({ tenantId, line, rule });
  }
  return [];
}

async function evaluateLine({ tenantId, line, rules }) {
  if (!(remainingAmountAbs(line) > 0)) {
    return { line, outcome: "SKIPPED", reasonCode: "NO_REMAINING_AMOUNT", reasonMessage: "No remaining amount", rule: null, candidates: [], target: null, confidence: null };
  }
  for (const rule of rules) {
    if (!applicable(rule, line) || !effective(rule, line) || !textPrecheck(line, rule)) continue;
    const policy = policyCheck(line, rule);
    if (policy.blocked) {
      return { line, outcome: "POLICY_BLOCKED", rule, candidates: [], target: null, confidence: null, ...policy };
    }
    const action = u(rule.action_type);
    if (action === "QUEUE_EXCEPTION") {
      return { line, outcome: "RULE_QUEUE_EXCEPTION", reasonCode: "RULE_QUEUE_EXCEPTION", reasonMessage: "Rule requested queue", rule, candidates: [], target: null, confidence: null };
    }
    if (action === "AUTO_POST_TEMPLATE") {
      const target = getAutoPostTemplateTargetFromRule(rule, line);
      if (!target) {
        return {
          line,
          outcome: "POLICY_BLOCKED",
          reasonCode: "POLICY_BLOCKED",
          reasonMessage: "AUTO_POST_TEMPLATE requires actionPayload.postingTemplateId",
          rule,
          candidates: [],
          target: null,
          confidence: null,
        };
      }
      return {
        line,
        outcome: "AUTO_POST_READY",
        reasonCode: null,
        reasonMessage: null,
        rule,
        candidates: [],
        target,
        confidence: 95,
      };
    }
    if (action === "PROCESS_PAYMENT_RETURN") {
      const candidates = await candidatesForRule({ tenantId, line, rule });
      if (!candidates.length) continue;
      if (candidates.length > 1) {
        return {
          line,
          outcome: "AMBIGUOUS_TARGET",
          reasonCode: "AMBIGUOUS_TARGET",
          reasonMessage: `Return rule found ${candidates.length} payment line candidates`,
          rule,
          candidates,
          target: null,
          confidence: null,
        };
      }
      const target = candidates[0];
      return {
        line,
        outcome: "AUTO_RETURN_READY",
        reasonCode: null,
        reasonMessage: null,
        rule,
        candidates,
        target,
        confidence: Number(target.confidence ?? 85),
      };
    }
    if (action === "AUTO_MATCH_PAYMENT_LINE_WITH_DIFFERENCE") {
      const differenceProfileId = getDifferenceProfileIdFromRule(rule);
      if (!differenceProfileId) {
        return {
          line,
          outcome: "POLICY_BLOCKED",
          reasonCode: "POLICY_BLOCKED",
          reasonMessage:
            "AUTO_MATCH_PAYMENT_LINE_WITH_DIFFERENCE requires actionPayload.differenceProfileId",
          rule,
          candidates: [],
          target: null,
          confidence: null,
        };
      }
      const candidates = await candidatesForRule({ tenantId, line, rule });
      if (!candidates.length) continue;
      if (candidates.length > 1) {
        return {
          line,
          outcome: "AMBIGUOUS_TARGET",
          reasonCode: "AMBIGUOUS_TARGET",
          reasonMessage: `Difference rule found ${candidates.length} payment line candidates`,
          rule,
          candidates,
          target: null,
          confidence: null,
        };
      }
      const target = { ...candidates[0], differenceProfileId };
      return {
        line,
        outcome: "AUTO_DIFF_READY",
        reasonCode: null,
        reasonMessage: null,
        rule,
        candidates: [target],
        target,
        confidence: Number(target.confidence ?? 85),
      };
    }
    const candidates = await candidatesForRule({ tenantId, line, rule });
    if (!candidates.length) continue;
    if (action === "SUGGEST_ONLY") {
      return { line, outcome: "SUGGEST_ONLY", reasonCode: null, reasonMessage: "Suggestions only", rule, candidates, target: null, confidence: null };
    }
    if (candidates.length > 1) {
      return { line, outcome: "AMBIGUOUS_TARGET", reasonCode: "AMBIGUOUS_TARGET", reasonMessage: `Rule found ${candidates.length} candidates`, rule, candidates, target: null, confidence: null };
    }
    const target = candidates[0];
    if (!target?.entityId || !target?.entityType) continue;
    if (action === "AUTO_MATCH_PAYMENT_BATCH" && u(target.entityType) !== "PAYMENT_BATCH") {
      return { line, outcome: "POLICY_BLOCKED", reasonCode: "POLICY_BLOCKED", reasonMessage: "Target type mismatch for payment rule", rule, candidates, target: null, confidence: null };
    }
    if (action === "AUTO_MATCH_JOURNAL" && u(target.entityType) !== "JOURNAL") {
      return { line, outcome: "POLICY_BLOCKED", reasonCode: "POLICY_BLOCKED", reasonMessage: "Target type mismatch for journal rule", rule, candidates, target: null, confidence: null };
    }
    return { line, outcome: "AUTO_MATCH_READY", reasonCode: null, reasonMessage: null, rule, candidates, target, confidence: Number(target.confidence ?? 80) };
  }
  return { line, outcome: "NO_RULE_MATCH", reasonCode: "NO_RULE_MATCH", reasonMessage: "No active rule produced a valid target", rule: null, candidates: [], target: null, confidence: null };
}

function evalRow(e) {
  const line = e?.line || {};
  return {
    statementLineId: parsePositiveInt(line.id),
    legalEntityId: parsePositiveInt(line.legal_entity_id),
    bankAccountId: parsePositiveInt(line.bank_account_id),
    txnDate: line.txn_date || null,
    referenceNo: line.reference_no || null,
    description: line.description || null,
    amount: toAmount(line.amount),
    remainingAmountAbs: remainingAmountAbs(line),
    reconStatus: line.recon_status || null,
    outcome: e?.outcome || null,
    reasonCode: e?.reasonCode || null,
    reasonMessage: e?.reasonMessage || null,
    rule: ruleTrace(e?.rule),
    candidateCount: Array.isArray(e?.candidates) ? e.candidates.length : 0,
    target: targetSummary(e?.target),
    confidence: e?.confidence === null || e?.confidence === undefined ? null : Number(Number(e.confidence).toFixed(2)),
    candidateSample: Array.isArray(e?.candidates) ? e.candidates.slice(0, 5).map(targetSummary) : [],
  };
}

function summarize(rows, mode) {
  const s = { scannedCount: rows.length, matchedCount: 0, reconciledCount: 0, exceptionCount: 0, skippedCount: 0, errorCount: 0 };
  for (const row of rows) {
    if (["AUTO_MATCH_READY", "AUTO_POST_READY", "AUTO_RETURN_READY", "AUTO_DIFF_READY"].includes(row.outcome)) {
      s.matchedCount += 1;
    }
    if (
      mode === "APPLY" &&
      ["RECONCILED", "AUTO_POSTED_RECONCILED", "RETURN_PROCESSED_RECONCILED", "DIFFERENCE_RECONCILED"].includes(
        row.outcome
      )
    ) {
      s.reconciledCount += 1;
    }
    if (["NO_RULE_MATCH", "AMBIGUOUS_TARGET", "POLICY_BLOCKED", "APPLY_ERROR", "RULE_QUEUE_EXCEPTION"].includes(row.outcome)) s.exceptionCount += 1;
    if (row.outcome === "SKIPPED") s.skippedCount += 1;
    if (row.outcome === "ERROR") s.errorCount += 1;
  }
  return s;
}

async function findExistingRun(tenantId, runRequestId) {
  if (!runRequestId) return null;
  const res = await query(
    `SELECT * FROM bank_reconciliation_auto_runs WHERE tenant_id = ? AND run_request_id = ? LIMIT 1`,
    [tenantId, runRequestId]
  );
  const row = res.rows?.[0] || null;
  return row ? { ...row, payload_json: parseJson(row.payload_json, null) } : null;
}

async function insertRunLog({ tenantId, filters, mode, status, runRequestId = null, summary: s, rows, userId = null }) {
  const payload = { summary: s, rows: (rows || []).slice(0, 200) };
  const ins = await query(
    `INSERT INTO bank_reconciliation_auto_runs (
        tenant_id, legal_entity_id, bank_account_id, run_request_id, run_mode, status, date_from, date_to,
        scanned_count, matched_count, reconciled_count, exception_count, skipped_count, error_count,
        payload_json, created_by_user_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      tenantId,
      filters.legalEntityId || null,
      filters.bankAccountId || null,
      runRequestId || null,
      mode,
      status,
      filters.dateFrom || null,
      filters.dateTo || null,
      Number(s.scannedCount || 0),
      Number(s.matchedCount || 0),
      Number(s.reconciledCount || 0),
      Number(s.exceptionCount || 0),
      Number(s.skippedCount || 0),
      Number(s.errorCount || 0),
      safeJson(payload),
      userId || null,
    ]
  );
  const id = parsePositiveInt(ins.rows?.insertId);
  const res = await query(`SELECT * FROM bank_reconciliation_auto_runs WHERE tenant_id = ? AND id = ? LIMIT 1`, [tenantId, id]);
  const row = res.rows?.[0] || null;
  return row ? { ...row, payload_json: parseJson(row.payload_json, null) } : null;
}

function exceptionSeverityFromOutcome(outcome) {
  if (outcome === "POLICY_BLOCKED" || outcome === "APPLY_ERROR") return "HIGH";
  if (outcome === "AMBIGUOUS_TARGET") return "MEDIUM";
  return "LOW";
}

async function queueExceptionFromEval({ tenantId, evaluation, userId }) {
  const line = evaluation.line;
  const exception = await upsertReconciliationException({
    tenantId,
    legalEntityId: line.legal_entity_id,
    statementLineId: line.id,
    bankAccountId: line.bank_account_id,
    reasonCode: evaluation.reasonCode || "NO_RULE_MATCH",
    reasonMessage: evaluation.reasonMessage || null,
    matchedRuleId: parsePositiveInt(evaluation?.rule?.id) || null,
    suggestedActionType:
      evaluation.outcome === "NO_RULE_MATCH"
        ? "MANUAL_MATCH"
        : evaluation.outcome === "AMBIGUOUS_TARGET"
          ? "MANUAL_REVIEW"
          : "MANUAL_REVIEW",
    suggestedPayload: {
      outcome: evaluation.outcome,
      rule: ruleTrace(evaluation.rule),
      candidateCount: Array.isArray(evaluation.candidates) ? evaluation.candidates.length : 0,
      candidateSample: (evaluation.candidates || []).slice(0, 5).map(targetSummary),
      target: targetSummary(evaluation.target),
    },
    severity: exceptionSeverityFromOutcome(evaluation.outcome),
    userId,
  });
  return parsePositiveInt(exception?.id) || null;
}

export async function previewBankReconciliationAutoRun({
  req,
  tenantId,
  filters,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const lines = await loadLines({ req, tenantId, filters, buildScopeFilter, assertScopeAccess });
  const rules = await listActiveRulesForAutomation({
    tenantId,
    legalEntityId: filters.legalEntityId || null,
    bankAccountId: filters.bankAccountId || null,
    asOfDate: filters.dateTo || filters.dateFrom || null,
  });

  const rows = [];
  for (const line of lines) {
    const evaluation = await evaluateLine({ tenantId, line, rules });
    rows.push(evalRow(evaluation));
  }
  const s = { ...summarize(rows, "PREVIEW"), rulesEvaluated: rules.length };
  const run = await insertRunLog({
    tenantId,
    filters,
    mode: "PREVIEW",
    status: "SUCCESS",
    summary: s,
    rows,
    userId: filters.userId || null,
  });

  return { run, summary: s, rows };
}

export async function applyBankReconciliationAutoRun({
  req,
  tenantId,
  filters,
  runRequestId = null,
  buildScopeFilter,
  assertScopeAccess,
}) {
  const existing = await findExistingRun(tenantId, runRequestId || null);
  if (existing) {
    return {
      replay: true,
      run: existing,
      summary: existing.payload_json?.summary || null,
      rows: existing.payload_json?.rows || [],
    };
  }

  const lines = await loadLines({ req, tenantId, filters, buildScopeFilter, assertScopeAccess });
  const rules = await listActiveRulesForAutomation({
    tenantId,
    legalEntityId: filters.legalEntityId || null,
    bankAccountId: filters.bankAccountId || null,
    asOfDate: filters.dateTo || filters.dateFrom || null,
  });

  let status = "SUCCESS";
  const rows = [];
  for (const line of lines) {
    try {
      const evaluation = await evaluateLine({ tenantId, line, rules });
      if (evaluation.outcome === "AUTO_MATCH_READY" && evaluation.target) {
        try {
          const matchResult = await matchReconciliationLine({
            req,
            tenantId,
            lineId: line.id,
            matchInput: {
              matchType: "AUTO_RULE",
              matchedEntityType: evaluation.target.entityType,
              matchedEntityId: evaluation.target.entityId,
              matchedAmount: remainingAmountAbs(line),
              notes: `Auto matched by rule ${evaluation.rule?.rule_code || evaluation.rule?.id}`,
              reconciliationMethod: "RULE",
              reconciliationRuleId: parsePositiveInt(evaluation.rule?.id) || null,
              reconciliationConfidence:
                evaluation.confidence === null || evaluation.confidence === undefined
                  ? null
                  : Number(Number(evaluation.confidence).toFixed(2)),
            },
            userId: filters.userId || null,
            assertScopeAccess,
          });
          rows.push({
            ...evalRow(evaluation),
            outcome: "RECONCILED",
            reconStatus: matchResult?.line?.recon_status || "MATCHED",
            exceptionId: null,
          });
          continue;
        } catch (err) {
          evaluation.outcome = "APPLY_ERROR";
          evaluation.reasonCode = "APPLY_ERROR";
          evaluation.reasonMessage = err?.message || "Auto apply failed";
          status = "PARTIAL";
        }
      }

      if (evaluation.outcome === "AUTO_POST_READY" && evaluation.target) {
        try {
          const autoPostResult = await autoPostTemplateAndReconcileStatementLine({
            req,
            tenantId,
            lineId: line.id,
            templateId: evaluation.target.entityId,
            userId: filters.userId || null,
            ruleId: parsePositiveInt(evaluation.rule?.id) || null,
            confidence:
              evaluation.confidence === null || evaluation.confidence === undefined
                ? null
                : Number(Number(evaluation.confidence).toFixed(2)),
            assertScopeAccess,
          });
          rows.push({
            ...evalRow(evaluation),
            outcome: "AUTO_POSTED_RECONCILED",
            reconStatus: autoPostResult?.line?.recon_status || "MATCHED",
            target: {
              entityType: "JOURNAL",
              entityId: parsePositiveInt(autoPostResult?.journal?.id) || null,
              amount: remainingAmountAbs(line),
              displayRef: autoPostResult?.journal?.journal_no || null,
              displayText: evaluation.target?.displayText || "Auto-posted journal",
            },
            candidateCount: 0,
            candidateSample: [],
            exceptionId: null,
            autoPosting: {
              id: parsePositiveInt(autoPostResult?.auto_posting?.id) || null,
              templateId: parsePositiveInt(autoPostResult?.template?.id) || evaluation.target.entityId,
              journalId: parsePositiveInt(autoPostResult?.journal?.id) || null,
              idempotent: Boolean(autoPostResult?.idempotent),
              reconciliationIdempotent: Boolean(autoPostResult?.reconciliation?.idempotent),
            },
          });
          continue;
        } catch (err) {
          evaluation.outcome = "APPLY_ERROR";
          evaluation.reasonCode = "APPLY_ERROR";
          evaluation.reasonMessage = err?.message || "Auto-post apply failed";
          status = "PARTIAL";
        }
      }

      if (evaluation.outcome === "AUTO_RETURN_READY" && evaluation.target) {
        try {
          const returnResult = await processPaymentReturnFromStatementLine({
            req,
            tenantId,
            lineId: line.id,
            userId: filters.userId || null,
            input: {
              paymentBatchLineId: evaluation.target.entityId,
              eventType:
                evaluation.rule?.action_payload_json?.eventType ??
                evaluation.rule?.action_payload_json?.event_type ??
                "PAYMENT_RETURNED",
              reasonCode:
                evaluation.rule?.action_payload_json?.reasonCode ??
                evaluation.rule?.action_payload_json?.reason_code ??
                null,
              reasonMessage:
                evaluation.rule?.action_payload_json?.reasonMessage ??
                evaluation.rule?.action_payload_json?.reason_message ??
                null,
            },
            ruleId: parsePositiveInt(evaluation.rule?.id) || null,
            confidence:
              evaluation.confidence === null || evaluation.confidence === undefined
                ? null
                : Number(Number(evaluation.confidence).toFixed(2)),
            assertScopeAccess,
          });
          rows.push({
            ...evalRow(evaluation),
            outcome: "RETURN_PROCESSED_RECONCILED",
            reconStatus: returnResult?.reconciliation?.line?.recon_status || "MATCHED",
            target: {
              entityType: "PAYMENT_BATCH",
              entityId: parsePositiveInt(returnResult?.paymentBatchId) || null,
              paymentBatchId: parsePositiveInt(returnResult?.paymentBatchId) || null,
              amount: remainingAmountAbs(line),
              displayRef: evaluation.target?.displayRef || null,
              displayText: evaluation.target?.displayText || "B08-B return",
            },
            returnProcessing: {
              paymentBatchLineId: parsePositiveInt(returnResult?.paymentBatchLineId) || null,
              eventId: parsePositiveInt(returnResult?.returnEvent?.id) || null,
              eventIdempotent: Boolean(returnResult?.returnEventIdempotent),
            },
            exceptionId: null,
          });
          continue;
        } catch (err) {
          evaluation.outcome = "APPLY_ERROR";
          evaluation.reasonCode = "APPLY_ERROR";
          evaluation.reasonMessage = err?.message || "B08-B return processing failed";
          status = "PARTIAL";
        }
      }

      if (evaluation.outcome === "AUTO_DIFF_READY" && evaluation.target) {
        try {
          const diffResult = await autoMatchPaymentLineWithDifferenceAndReconcile({
            req,
            tenantId,
            lineId: line.id,
            paymentBatchLineId: evaluation.target.entityId,
            differenceProfileId: evaluation.target.differenceProfileId,
            userId: filters.userId || null,
            ruleId: parsePositiveInt(evaluation.rule?.id) || null,
            confidence:
              evaluation.confidence === null || evaluation.confidence === undefined
                ? null
                : Number(Number(evaluation.confidence).toFixed(2)),
            assertScopeAccess,
          });
          rows.push({
            ...evalRow(evaluation),
            outcome: "DIFFERENCE_RECONCILED",
            reconStatus: diffResult?.statementLine?.recon_status || "MATCHED",
            difference: diffResult?.difference || null,
            differenceAdjustment: {
              id: parsePositiveInt(diffResult?.adjustment?.id) || null,
              journalEntryId:
                parsePositiveInt(diffResult?.adjustment?.journal_entry_id) ||
                parsePositiveInt(diffResult?.journal?.id) ||
                null,
              idempotent: Boolean(diffResult?.idempotent),
            },
            exceptionId: null,
          });
          continue;
        } catch (err) {
          evaluation.outcome = "APPLY_ERROR";
          evaluation.reasonCode = "APPLY_ERROR";
          evaluation.reasonMessage = err?.message || "B08-B difference auto-match failed";
          status = "PARTIAL";
        }
      }

      let exceptionId = null;
      if (["NO_RULE_MATCH", "AMBIGUOUS_TARGET", "POLICY_BLOCKED", "APPLY_ERROR", "RULE_QUEUE_EXCEPTION"].includes(evaluation.outcome)) {
        exceptionId = await queueExceptionFromEval({
          tenantId,
          evaluation,
          userId: filters.userId || null,
        });
        if (status === "SUCCESS") status = "PARTIAL";
      } else if (evaluation.outcome === "SUGGEST_ONLY" && status === "SUCCESS") {
        status = "PARTIAL";
      }
      rows.push({ ...evalRow(evaluation), exceptionId });
    } catch (err) {
      status = "PARTIAL";
      rows.push({
        statementLineId: parsePositiveInt(line.id),
        legalEntityId: parsePositiveInt(line.legal_entity_id),
        bankAccountId: parsePositiveInt(line.bank_account_id),
        txnDate: line.txn_date || null,
        referenceNo: line.reference_no || null,
        description: line.description || null,
        amount: toAmount(line.amount),
        remainingAmountAbs: remainingAmountAbs(line),
        reconStatus: line.recon_status || null,
        outcome: "ERROR",
        reasonCode: "ENGINE_ERROR",
        reasonMessage: err?.message || "Unexpected rule engine error",
        rule: null,
        candidateCount: 0,
        target: null,
        confidence: null,
        candidateSample: [],
      });
    }
  }

  const s = { ...summarize(rows, "APPLY"), rulesEvaluated: rules.length };
  const run = await insertRunLog({
    tenantId,
    filters,
    mode: "APPLY",
    status,
    runRequestId: runRequestId || null,
    summary: s,
    rows,
    userId: filters.userId || null,
  });
  return { replay: false, run, summary: s, rows };
}

export default {
  previewBankReconciliationAutoRun,
  applyBankReconciliationAutoRun,
};
