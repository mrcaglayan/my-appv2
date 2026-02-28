import BasePayrollProviderAdapter from "./base.adapter.js";

function parseCsvLine(line) {
  const out = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(current);
      current = "";
      continue;
    }
    current += ch;
  }
  out.push(current);
  return out.map((v) => String(v ?? "").trim());
}

function money(value, fieldName) {
  const normalized = String(value ?? "")
    .trim()
    .replace(/,/g, "");
  if (!normalized) return 0;
  const parsed = Number(normalized);
  if (!Number.isFinite(parsed)) throw new Error(`${fieldName} is invalid number`);
  return Number(parsed.toFixed(6));
}

function amountAlias(headerIndex, row, aliases, defaultValue = 0) {
  for (const key of aliases) {
    const idx = headerIndex[key];
    if (idx === undefined) continue;
    const raw = row[idx];
    if (raw === undefined || raw === null || String(raw).trim() === "") continue;
    return money(raw, key);
  }
  return defaultValue;
}

function textAlias(headerIndex, row, aliases) {
  for (const key of aliases) {
    const idx = headerIndex[key];
    if (idx === undefined) continue;
    const raw = row[idx];
    const text = String(raw ?? "").trim();
    if (text) return text;
  }
  return null;
}

function normalizeEmployeeRecord(row, headerIndex, rowNo) {
  const externalEmployeeId = textAlias(headerIndex, row, ["external_employee_id"]);
  const externalEmployeeCode = textAlias(headerIndex, row, ["external_employee_code", "employee_code"]);
  const employeeName = textAlias(headerIndex, row, ["employee_name"]);
  const employeeEmail = textAlias(headerIndex, row, ["employee_email"]);
  const costCenterCode = textAlias(headerIndex, row, ["cost_center_code"]);

  if (!externalEmployeeId) {
    throw new Error(`Row ${rowNo}: external_employee_id is required`);
  }
  if (!employeeName) {
    throw new Error(`Row ${rowNo}: employee_name is required`);
  }

  const grossPay = amountAlias(headerIndex, row, ["gross_pay", "gross_amount"]);
  const netPay = amountAlias(headerIndex, row, ["net_pay", "net_pay_amount"]);
  const overtimePay = amountAlias(headerIndex, row, ["overtime_pay"]);
  const bonusPay = amountAlias(headerIndex, row, ["bonus_pay"]);
  const allowancesTotal = amountAlias(headerIndex, row, ["allowances_total"]);
  const baseSalary = amountAlias(
    headerIndex,
    row,
    ["base_salary"],
    Number((grossPay - overtimePay - bonusPay - allowancesTotal).toFixed(6))
  );
  const employeeTax = amountAlias(headerIndex, row, ["employee_tax", "employee_tax_amount"]);
  const employeeSocialSecurity = amountAlias(headerIndex, row, [
    "employee_social_security",
    "employee_social_security_amount",
    "employee_ss_amount",
  ]);
  const employerTax = amountAlias(headerIndex, row, ["employer_tax", "employer_tax_amount"]);
  const employerSocialSecurity = amountAlias(headerIndex, row, [
    "employer_social_security",
    "employer_social_security_amount",
    "employer_ss_amount",
  ]);
  const otherDeductionsExplicit = amountAlias(
    headerIndex,
    row,
    ["other_deductions", "other_deductions_amount", "deductions_amount"],
    NaN
  );
  const computedOtherDeductions = Number(
    Math.max(0, grossPay - employeeTax - employeeSocialSecurity - netPay).toFixed(6)
  );
  const otherDeductions = Number.isFinite(otherDeductionsExplicit)
    ? otherDeductionsExplicit
    : computedOtherDeductions;

  const grossExpected = Number((baseSalary + overtimePay + bonusPay + allowancesTotal).toFixed(6));
  const netExpected = Number(
    (grossPay - employeeTax - employeeSocialSecurity - otherDeductions).toFixed(6)
  );
  if (Math.abs(grossExpected - grossPay) > 0.05) {
    throw new Error(
      `Row ${rowNo}: gross_pay mismatch (expected ${grossExpected.toFixed(2)}, got ${grossPay.toFixed(2)})`
    );
  }
  if (Math.abs(netExpected - netPay) > 0.05) {
    throw new Error(
      `Row ${rowNo}: net_pay mismatch (expected ${netExpected.toFixed(2)}, got ${netPay.toFixed(2)})`
    );
  }

  return {
    external_employee_id: externalEmployeeId,
    external_employee_code: externalEmployeeCode,
    employee_name: employeeName,
    employee_email: employeeEmail,
    cost_center_code: costCenterCode,
    base_salary: baseSalary,
    overtime_pay: overtimePay,
    bonus_pay: bonusPay,
    allowances_total: allowancesTotal,
    gross_pay: grossPay,
    employee_tax: employeeTax,
    employee_social_security: employeeSocialSecurity,
    other_deductions: otherDeductions,
    employer_tax: employerTax,
    employer_social_security: employerSocialSecurity,
    net_pay: netPay,
    raw_row_json: null,
  };
}

class GenericCsvPayrollAdapter extends BasePayrollProviderAdapter {
  parseRaw(rawPayloadText) {
    const text = String(rawPayloadText || "").replace(/\r\n/g, "\n").trim();
    if (!text) {
      throw new Error("CSV payload is empty");
    }
    const lines = text.split("\n").filter((line) => line.trim() !== "");
    if (lines.length < 2) {
      throw new Error("CSV payload must include header and at least one row");
    }
    const header = parseCsvLine(lines[0]).map((v) => v.toLowerCase());
    const rows = lines.slice(1).map((line) => parseCsvLine(line));
    return { header, rows };
  }

  validateSchema(parsed) {
    const errors = [];
    const warnings = [];
    if (!parsed || !Array.isArray(parsed.header) || !Array.isArray(parsed.rows)) {
      return { errors: ["Parsed CSV payload is invalid"], warnings };
    }
    const header = parsed.header || [];
    for (const col of ["external_employee_id", "employee_name"]) {
      if (!header.includes(col)) errors.push(`Missing CSV column: ${col}`);
    }
    const hasGross = header.includes("gross_pay") || header.includes("gross_amount");
    const hasNet = header.includes("net_pay") || header.includes("net_pay_amount");
    if (!hasGross) errors.push("Missing CSV column: gross_pay or gross_amount");
    if (!hasNet) errors.push("Missing CSV column: net_pay or net_pay_amount");
    if (!header.includes("external_employee_code") && !header.includes("employee_code")) {
      warnings.push(
        "external_employee_code/employee_code column is missing; fallback mapping by code will not be available"
      );
    }
    return { errors, warnings };
  }

  normalizePayrollResults(parsed, context = {}) {
    const header = parsed?.header || [];
    const rows = parsed?.rows || [];
    const headerIndex = Object.fromEntries(header.map((col, idx) => [col, idx]));
    const employees = [];

    for (let i = 0; i < rows.length; i += 1) {
      const rawCols = rows[i];
      if ((rawCols || []).every((cell) => String(cell || "").trim() === "")) continue;
      const normalized = normalizeEmployeeRecord(rawCols, headerIndex, i + 2);
      normalized.raw_row_json = Object.fromEntries(
        header.map((col, idx) => [col, rawCols[idx] ?? ""])
      );
      employees.push(normalized);
    }

    if (employees.length === 0) {
      throw new Error("CSV payload has no valid employee rows");
    }

    const totalGross = Number(employees.reduce((s, r) => s + Number(r.gross_pay || 0), 0).toFixed(6));
    const totalNet = Number(employees.reduce((s, r) => s + Number(r.net_pay || 0), 0).toFixed(6));
    const totalEmployerTax = Number(
      employees.reduce((s, r) => s + Number(r.employer_tax || 0), 0).toFixed(6)
    );
    const totalEmployerSocialSecurity = Number(
      employees.reduce((s, r) => s + Number(r.employer_social_security || 0), 0).toFixed(6)
    );

    return {
      run: {
        payroll_period: context.payrollPeriod || null,
        pay_date: context.payDate || null,
        currency_code: context.currencyCode || null,
        source_batch_ref: context.sourceBatchRef || null,
      },
      employees,
      summary: {
        employee_count: employees.length,
        total_gross_pay: totalGross,
        total_net_pay: totalNet,
        total_employer_tax: totalEmployerTax,
        total_employer_social_security: totalEmployerSocialSecurity,
      },
    };
  }
}

export default GenericCsvPayrollAdapter;
