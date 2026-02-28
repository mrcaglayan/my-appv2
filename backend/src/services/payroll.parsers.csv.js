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
  return out.map((value) => String(value ?? "").trim());
}

function parseMoney(value, fieldName) {
  const normalized = String(value ?? "")
    .trim()
    .replace(/,/g, "");
  if (normalized === "") {
    return 0;
  }
  const parsed = Number(normalized);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${fieldName} is invalid number`);
  }
  return Number(parsed.toFixed(6));
}

const REQUIRED_COLUMNS = [
  "employee_code",
  "employee_name",
  "cost_center_code",
  "base_salary",
  "overtime_pay",
  "bonus_pay",
  "allowances_total",
  "gross_pay",
  "employee_tax",
  "employee_social_security",
  "other_deductions",
  "employer_tax",
  "employer_social_security",
  "net_pay",
];

export function parsePayrollCsv(csvText) {
  const text = String(csvText || "").replace(/\r\n/g, "\n").trim();
  if (!text) {
    throw new Error("CSV is empty");
  }

  const lines = text.split("\n").filter((line) => line.trim() !== "");
  if (lines.length < 2) {
    throw new Error("CSV must include header and at least one row");
  }

  const header = parseCsvLine(lines[0]).map((value) => value.toLowerCase());
  for (const column of REQUIRED_COLUMNS) {
    if (!header.includes(column)) {
      throw new Error(`Missing CSV column: ${column}`);
    }
  }

  const columnIndex = Object.fromEntries(REQUIRED_COLUMNS.map((c) => [c, header.indexOf(c)]));
  const rows = [];

  for (let lineIndex = 1; lineIndex < lines.length; lineIndex += 1) {
    const cols = parseCsvLine(lines[lineIndex]);
    if (cols.every((cell) => String(cell || "").trim() === "")) {
      continue;
    }

    const raw = {};
    for (const column of REQUIRED_COLUMNS) {
      raw[column] = cols[columnIndex[column]] ?? "";
    }

    const employeeCode = String(raw.employee_code || "").trim();
    const employeeName = String(raw.employee_name || "").trim();
    const costCenterCode = String(raw.cost_center_code || "").trim() || null;

    if (!employeeCode) {
      throw new Error(`Row ${lineIndex + 1}: employee_code is required`);
    }
    if (!employeeName) {
      throw new Error(`Row ${lineIndex + 1}: employee_name is required`);
    }

    const row = {
      line_no: rows.length + 1,
      employee_code: employeeCode,
      employee_name: employeeName,
      cost_center_code: costCenterCode,
      base_salary: parseMoney(raw.base_salary, `Row ${lineIndex + 1} base_salary`),
      overtime_pay: parseMoney(raw.overtime_pay, `Row ${lineIndex + 1} overtime_pay`),
      bonus_pay: parseMoney(raw.bonus_pay, `Row ${lineIndex + 1} bonus_pay`),
      allowances_total: parseMoney(raw.allowances_total, `Row ${lineIndex + 1} allowances_total`),
      gross_pay: parseMoney(raw.gross_pay, `Row ${lineIndex + 1} gross_pay`),
      employee_tax: parseMoney(raw.employee_tax, `Row ${lineIndex + 1} employee_tax`),
      employee_social_security: parseMoney(
        raw.employee_social_security,
        `Row ${lineIndex + 1} employee_social_security`
      ),
      other_deductions: parseMoney(
        raw.other_deductions,
        `Row ${lineIndex + 1} other_deductions`
      ),
      employer_tax: parseMoney(raw.employer_tax, `Row ${lineIndex + 1} employer_tax`),
      employer_social_security: parseMoney(
        raw.employer_social_security,
        `Row ${lineIndex + 1} employer_social_security`
      ),
      net_pay: parseMoney(raw.net_pay, `Row ${lineIndex + 1} net_pay`),
      raw_row_json: raw,
    };

    const grossExpected = Number(
      (
        row.base_salary +
        row.overtime_pay +
        row.bonus_pay +
        row.allowances_total
      ).toFixed(6)
    );
    const netExpected = Number(
      (
        row.gross_pay -
        row.employee_tax -
        row.employee_social_security -
        row.other_deductions
      ).toFixed(6)
    );

    if (Math.abs(grossExpected - row.gross_pay) > 0.05) {
      throw new Error(
        `Row ${lineIndex + 1}: gross_pay mismatch (expected ${grossExpected.toFixed(2)}, got ${Number(
          row.gross_pay
        ).toFixed(2)})`
      );
    }
    if (Math.abs(netExpected - row.net_pay) > 0.05) {
      throw new Error(
        `Row ${lineIndex + 1}: net_pay mismatch (expected ${netExpected.toFixed(2)}, got ${Number(
          row.net_pay
        ).toFixed(2)})`
      );
    }

    rows.push(row);
  }

  if (rows.length === 0) {
    throw new Error("CSV has no valid rows");
  }

  return rows;
}

