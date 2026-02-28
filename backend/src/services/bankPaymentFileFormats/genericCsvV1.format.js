import { badRequest } from "../../routes/_utils.js";

function splitCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
      continue;
    }
    if (ch === ",") {
      out.push(cur);
      cur = "";
      continue;
    }
    if (ch === '"') {
      inQuotes = true;
      continue;
    }
    cur += ch;
  }
  out.push(cur);
  return out;
}

function normalizeAckStatus(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function toAmountOrNull(value) {
  if (value === undefined || value === null || String(value).trim() === "") return null;
  const n = Number(value);
  if (!Number.isFinite(n)) {
    throw badRequest("Ack file contains invalid numeric ack_amount");
  }
  return Number(n.toFixed(6));
}

function toDateTimeOrNull(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 19).replace("T", " ");
}

export function parseAcknowledgement({ ackText }) {
  const text = String(ackText || "").trim();
  if (!text) {
    throw badRequest("ackText is required");
  }
  const lines = text.split(/\r?\n/).filter((line) => String(line).trim() !== "");
  if (lines.length < 2) {
    throw badRequest("Ack CSV must include a header and at least one data row");
  }

  const headers = splitCsvLine(lines[0]).map((h) => String(h || "").trim());
  const idx = Object.fromEntries(headers.map((h, i) => [h, i]));

  const hasLineRef = idx.line_ref !== undefined;
  const hasBatchLinePair = idx.batch_no !== undefined && idx.line_no !== undefined;
  if (!hasLineRef && !hasBatchLinePair) {
    throw badRequest("Ack CSV must include line_ref or batch_no + line_no columns");
  }
  if (idx.ack_status === undefined) {
    throw badRequest("Ack CSV missing ack_status column");
  }

  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const rawLine = lines[i];
    const cols = splitCsvLine(rawLine);
    rows.push({
      row_no: i + 1,
      line_ref: hasLineRef ? String(cols[idx.line_ref] || "").trim() : null,
      batch_no: idx.batch_no !== undefined ? String(cols[idx.batch_no] || "").trim() : null,
      line_no:
        idx.line_no !== undefined && String(cols[idx.line_no] || "").trim() !== ""
          ? Number(cols[idx.line_no])
          : null,
      ack_status: normalizeAckStatus(cols[idx.ack_status]),
      ack_amount: idx.ack_amount !== undefined ? toAmountOrNull(cols[idx.ack_amount]) : null,
      bank_reference:
        idx.bank_reference !== undefined ? String(cols[idx.bank_reference] || "").trim() : null,
      ack_code: idx.ack_code !== undefined ? String(cols[idx.ack_code] || "").trim() : null,
      ack_message: idx.ack_message !== undefined ? String(cols[idx.ack_message] || "").trim() : null,
      executed_at: idx.executed_at !== undefined ? toDateTimeOrNull(cols[idx.executed_at]) : null,
      currency_code:
        idx.currency_code !== undefined ? String(cols[idx.currency_code] || "").trim().toUpperCase() : null,
      raw_row: rawLine,
    });
  }

  return { rows };
}

export default {
  file_format_code: "GENERIC_CSV_V1",
  parseAcknowledgement,
};
