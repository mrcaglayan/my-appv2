class BasePayrollProviderAdapter {
  constructor({ settings = {}, adapterVersion = "v1", providerCode }) {
    this.settings = settings || {};
    this.adapterVersion = adapterVersion || "v1";
    this.providerCode = String(providerCode || "")
      .trim()
      .toUpperCase();
  }

  parseRaw(_rawPayloadText, _context = {}) {
    throw new Error("parseRaw() not implemented");
  }

  validateSchema(_parsed, _context = {}) {
    return { errors: [], warnings: [] };
  }

  normalizePayrollResults(_parsed, _context = {}) {
    throw new Error("normalizePayrollResults() not implemented");
  }
}

export default BasePayrollProviderAdapter;
