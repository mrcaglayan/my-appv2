import genericCsvV1 from "./genericCsvV1.format.js";

export function getBankPaymentFileFormat(formatCode) {
  const code = String(formatCode || "GENERIC_CSV_V1")
    .trim()
    .toUpperCase();

  if (code === "GENERIC_CSV_V1") {
    return genericCsvV1;
  }

  const err = new Error(`Unsupported bank payment file format: ${code}`);
  err.status = 400;
  throw err;
}

export default {
  getBankPaymentFileFormat,
};
