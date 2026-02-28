import GenericCsvPayrollAdapter from "./payroll.providers.adapters/genericCsv.adapter.js";
import GenericJsonPayrollAdapter from "./payroll.providers.adapters/genericJson.adapter.js";

const ADAPTERS = Object.freeze({
  GENERIC_CSV: GenericCsvPayrollAdapter,
  GENERIC_JSON: GenericJsonPayrollAdapter,
});

export function listSupportedPayrollProviderAdapters() {
  return Object.keys(ADAPTERS).map((providerCode) => ({
    provider_code: providerCode,
    adapter_class: ADAPTERS[providerCode].name,
  }));
}

export function getPayrollProviderAdapter(providerCode, { settings = {}, adapterVersion = "v1" } = {}) {
  const code = String(providerCode || "")
    .trim()
    .toUpperCase();
  const AdapterClass = ADAPTERS[code];
  if (!AdapterClass) {
    const err = new Error(`Unsupported payroll provider adapter: ${code}`);
    err.status = 400;
    throw err;
  }
  return new AdapterClass({
    settings,
    adapterVersion,
    providerCode: code,
  });
}

export default {
  listSupportedPayrollProviderAdapters,
  getPayrollProviderAdapter,
};
