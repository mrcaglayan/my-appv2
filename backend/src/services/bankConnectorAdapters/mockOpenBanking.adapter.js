function requireToken(credentials = {}) {
  const token = String(credentials?.token || "").trim();
  if (!token) {
    const err = new Error("Missing API token");
    err.status = 400;
    throw err;
  }
  return token;
}

async function testConnection({ config = {}, credentials = {} }) {
  requireToken(credentials);
  return {
    ok: true,
    providerCode: "MOCK_OB",
    connectorType: "OPEN_BANKING",
    remoteBankName: String(config?.bankName || "Mock Open Banking"),
    checkedAt: new Date().toISOString(),
  };
}

async function pullStatements({
  config = {},
  credentials = {},
  cursor = null,
  fromDate = null,
  toDate = null,
}) {
  requireToken(credentials);
  const bookingDate = String(toDate || fromDate || new Date().toISOString().slice(0, 10));
  const currencyCode = String(config?.currencyCode || "USD").trim().toUpperCase() || "USD";

  return {
    accounts: [
      {
        external_account_id: String(config?.externalAccountId || "EXT-ACC-001"),
        account_name: String(config?.externalAccountName || "Mock Operating Account"),
        currency_code: currencyCode,
        lines: [
          {
            external_txn_id: cursor
              ? `MOCK-TXN-${bookingDate}-002`
              : `MOCK-TXN-${bookingDate}-001`,
            booking_date: bookingDate,
            value_date: bookingDate,
            amount: -1250.0,
            currency_code: currencyCode,
            description: "Mock connector statement line",
            reference: `MOCK-${bookingDate}`,
            counterparty_name: "Mock Counterparty",
            balance_after: 100000.25,
          },
        ],
      },
    ],
    next_cursor: cursor ? null : `MOCKCURSOR-${bookingDate}`,
  };
}

export default {
  provider_code: "MOCK_OB",
  testConnection,
  pullStatements,
};
