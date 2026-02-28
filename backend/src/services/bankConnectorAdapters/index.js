import mockOpenBankingAdapter from "./mockOpenBanking.adapter.js";
import sandboxOpenBankingAdapter from "./sandboxOpenBanking.adapter.js";

function getBankConnectorAdapter(providerCode) {
  const normalized = String(providerCode || "").trim().toUpperCase();
  switch (normalized) {
    case "MOCK_OB":
      return mockOpenBankingAdapter;
    case "SANDBOX_OB":
      return sandboxOpenBankingAdapter;
    default: {
      const err = new Error(`Unsupported bank connector provider: ${providerCode}`);
      err.status = 400;
      throw err;
    }
  }
}

export { getBankConnectorAdapter };

export default {
  getBankConnectorAdapter,
};
