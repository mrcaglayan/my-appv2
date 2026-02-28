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
  return out.map((value) => String(value || "").trim());
}

function parseDateOnly(value, fieldName) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw new Error(`${fieldName} must be YYYY-MM-DD`);
  }
  const parsed = new Date(`${raw}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== raw) {
    throw new Error(`${fieldName} must be a valid date`);
  }
  return raw;
}

function parseDecimal(value, fieldName) {
  const raw = String(value ?? "")
    .trim()
    .replace(/,/g, "");
  if (!raw) {
    throw new Error(`${fieldName} is required`);
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${fieldName} is invalid number`);
  }
  return Number(parsed.toFixed(6));
}

function parseOptionalDecimal(value, fieldName) {
  const raw = String(value ?? "")
    .trim()
    .replace(/,/g, "");
  if (!raw) {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${fieldName} is invalid number`);
  }
  return Number(parsed.toFixed(6));
}

export function parseStatementCsv(csvText) {
  const normalizedText = String(csvText || "")
    .replace(/\r\n/g, "\n")
    .trim();

  if (!normalizedText) {
    throw new Error("CSV is empty");
  }

  const lines = normalizedText.split("\n").filter((line) => String(line).trim() !== "");
  if (lines.length < 2) {
    throw new Error("CSV must include header and at least one row");
  }

  const header = parseCsvLine(lines[0]).map((value) => value.toLowerCase());
  const requiredColumns = [
    "txn_date",
    "value_date",
    "description",
    "reference_no",
    "amount",
    "currency_code",
    "balance_after",
  ];

  for (const column of requiredColumns) {
    if (!header.includes(column)) {
      throw new Error(`Missing CSV column: ${column}`);
    }
  }

  const columnIndex = Object.fromEntries(
    requiredColumns.map((column) => [column, header.indexOf(column)])
  );

  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cols = parseCsvLine(lines[i]);
    if (cols.every((value) => value === "")) {
      continue;
    }

    const raw = {
      txn_date: cols[columnIndex.txn_date] ?? "",
      value_date: cols[columnIndex.value_date] ?? "",
      description: cols[columnIndex.description] ?? "",
      reference_no: cols[columnIndex.reference_no] ?? "",
      amount: cols[columnIndex.amount] ?? "",
      currency_code: cols[columnIndex.currency_code] ?? "",
      balance_after: cols[columnIndex.balance_after] ?? "",
    };

    const description = String(raw.description || "").trim();
    if (!description) {
      throw new Error(`Row ${i + 1}: description is required`);
    }

    const txnDate = parseDateOnly(raw.txn_date, `Row ${i + 1} txn_date`);
    if (!txnDate) {
      throw new Error(`Row ${i + 1} txn_date is required`);
    }

    const currencyCode = String(raw.currency_code || "").trim().toUpperCase();
    if (!/^[A-Z]{3}$/.test(currencyCode)) {
      throw new Error(`Row ${i + 1}: currency_code must be 3-letter code`);
    }

    rows.push({
      line_no: i,
      txn_date: txnDate,
      value_date: parseDateOnly(raw.value_date, `Row ${i + 1} value_date`),
      description,
      reference_no: String(raw.reference_no || "").trim() || null,
      amount: parseDecimal(raw.amount, `Row ${i + 1} amount`),
      currency_code: currencyCode,
      balance_after: parseOptionalDecimal(raw.balance_after, `Row ${i + 1} balance_after`),
      raw_row_json: raw,
    });
  }

  if (rows.length === 0) {
    throw new Error("CSV has no valid data rows");
  }

  return rows;
}

export default {
  parseStatementCsv,
};
