import BasePayrollProviderAdapter from "./base.adapter.js";

function toAmount(value, fieldName) {
  const normalized = value === undefined || value === null || value === "" ? 0 : Number(value);
  if (!Number.isFinite(normalized)) {
    throw new Error(`${fieldName} is invalid number`);
  }
  return Number(normalized.toFixed(6));
}

function normalizeRowAmounts(row, rowNo) {
  const grossPay = toAmount(row.gross_pay ?? row.gross_amount, `Row ${rowNo} gross`);
  const netPay = toAmount(row.net_pay ?? row.net_pay_amount, `Row ${rowNo} net`);
  const overtimePay = toAmount(row.overtime_pay, `Row ${rowNo} overtime_pay`);
  const bonusPay = toAmount(row.bonus_pay, `Row ${rowNo} bonus_pay`);
  const allowancesTotal = toAmount(row.allowances_total, `Row ${rowNo} allowances_total`);
  const baseSalaryRaw = row.base_salary;
  const baseSalary =
    baseSalaryRaw === undefined || baseSalaryRaw === null || baseSalaryRaw === ""
      ? Number((grossPay - overtimePay - bonusPay - allowancesTotal).toFixed(6))
      : toAmount(baseSalaryRaw, `Row ${rowNo} base_salary`);

  const employeeTax = toAmount(row.employee_tax ?? row.employee_tax_amount, `Row ${rowNo} employee_tax`);
  const employeeSocialSecurity = toAmount(
    row.employee_social_security ?? row.employee_social_security_amount ?? row.employee_ss_amount,
    `Row ${rowNo} employee_social_security`
  );
  const employerTax = toAmount(row.employer_tax ?? row.employer_tax_amount, `Row ${rowNo} employer_tax`);
  const employerSocialSecurity = toAmount(
    row.employer_social_security ?? row.employer_social_security_amount ?? row.employer_ss_amount,
    `Row ${rowNo} employer_social_security`
  );

  const otherDeductionsRaw =
    row.other_deductions ?? row.other_deductions_amount ?? row.deductions_amount;
  const otherDeductions =
    otherDeductionsRaw === undefined || otherDeductionsRaw === null || otherDeductionsRaw === ""
      ? Number(Math.max(0, grossPay - employeeTax - employeeSocialSecurity - netPay).toFixed(6))
      : toAmount(otherDeductionsRaw, `Row ${rowNo} other_deductions`);

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
  };
}

class GenericJsonPayrollAdapter extends BasePayrollProviderAdapter {
  parseRaw(rawPayloadText) {
    try {
      return JSON.parse(String(rawPayloadText || ""));
    } catch {
      throw new Error("Invalid JSON payload");
    }
  }

  validateSchema(parsed) {
    const errors = [];
    const warnings = [];
    if (!parsed || typeof parsed !== "object") {
      errors.push("JSON payload must be an object");
      return { errors, warnings };
    }
    const rows = Array.isArray(parsed.employees)
      ? parsed.employees
      : Array.isArray(parsed.rows)
        ? parsed.rows
        : null;
    if (!rows) {
      errors.push("JSON payload must include employees[] or rows[]");
      return { errors, warnings };
    }
    if (rows.length === 0) {
      errors.push("JSON payload employees[] is empty");
      return { errors, warnings };
    }
    const sample = rows[0] || {};
    if (!("external_employee_id" in sample)) errors.push("Missing field: external_employee_id");
    if (!("employee_name" in sample)) errors.push("Missing field: employee_name");
    if (!("gross_pay" in sample) && !("gross_amount" in sample)) {
      errors.push("Missing field: gross_pay or gross_amount");
    }
    if (!("net_pay" in sample) && !("net_pay_amount" in sample)) {
      errors.push("Missing field: net_pay or net_pay_amount");
    }
    if (!("external_employee_code" in sample) && !("employee_code" in sample)) {
      warnings.push(
        "external_employee_code/employee_code field missing; fallback mapping by code will not be available"
      );
    }
    return { errors, warnings };
  }

  normalizePayrollResults(parsed, context = {}) {
    const sourceRows = Array.isArray(parsed?.employees)
      ? parsed.employees
      : Array.isArray(parsed?.rows)
        ? parsed.rows
        : [];
    const employees = [];

    for (let i = 0; i < sourceRows.length; i += 1) {
      const row = sourceRows[i] || {};
      const rowNo = i + 1;
      const externalEmployeeId = String(row.external_employee_id || "").trim();
      const employeeName = String(row.employee_name || "").trim();
      if (!externalEmployeeId) throw new Error(`Row ${rowNo}: external_employee_id is required`);
      if (!employeeName) throw new Error(`Row ${rowNo}: employee_name is required`);

      const amounts = normalizeRowAmounts(row, rowNo);
      employees.push({
        external_employee_id: externalEmployeeId,
        external_employee_code:
          String(row.external_employee_code ?? row.employee_code ?? "").trim() || null,
        employee_name: employeeName,
        employee_email: String(row.employee_email || "").trim() || null,
        cost_center_code: String(row.cost_center_code || "").trim() || null,
        ...amounts,
        raw_row_json: row,
      });
    }

    if (employees.length === 0) throw new Error("JSON payload has no valid employee rows");

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
        total_gross_pay: Number(
          employees.reduce((s, r) => s + Number(r.gross_pay || 0), 0).toFixed(6)
        ),
        total_net_pay: Number(
          employees.reduce((s, r) => s + Number(r.net_pay || 0), 0).toFixed(6)
        ),
        total_employer_tax: Number(
          employees.reduce((s, r) => s + Number(r.employer_tax || 0), 0).toFixed(6)
        ),
        total_employer_social_security: Number(
          employees.reduce((s, r) => s + Number(r.employer_social_security || 0), 0).toFixed(6)
        ),
      },
    };
  }
}

export default GenericJsonPayrollAdapter;
