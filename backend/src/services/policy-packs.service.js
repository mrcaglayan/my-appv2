const CARI_REQUIRED_PURPOSE_CODES = Object.freeze([
  "CARI_AR_CONTROL",
  "CARI_AR_OFFSET",
  "CARI_AP_CONTROL",
  "CARI_AP_OFFSET",
]);

const SHAREHOLDER_REQUIRED_PURPOSE_CODES = Object.freeze([
  "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
  "SHAREHOLDER_COMMITMENT_DEBIT_PARENT",
]);

const PACKS = Object.freeze([
  Object.freeze({
    packId: "TR_UNIFORM_V1",
    countryIso2: "TR",
    label: "Turkey Uniform Starter v1",
    locked: true,
    modules: Object.freeze([
      Object.freeze({
        moduleKey: "cariPosting",
        label: "Cari posting",
        requiredPurposeCodes: CARI_REQUIRED_PURPOSE_CODES,
        purposeTargets: Object.freeze([
          Object.freeze({
            purposeCode: "CARI_AR_CONTROL",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "ASSET",
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["120"]),
            }),
            suggestCreate: Object.freeze({
              code: "120",
              name: "Trade Receivables",
              accountType: "ASSET",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AR_OFFSET",
            rules: Object.freeze({
              allowPosting: true,
              normalSide: "CREDIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["600"]),
            }),
            suggestCreate: Object.freeze({
              code: "600",
              name: "Domestic Sales",
              accountType: "REVENUE",
              normalSide: "CREDIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AP_CONTROL",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "LIABILITY",
              normalSide: "CREDIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["320"]),
            }),
            suggestCreate: Object.freeze({
              code: "320",
              name: "Trade Payables",
              accountType: "LIABILITY",
              normalSide: "CREDIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AP_OFFSET",
            rules: Object.freeze({
              allowPosting: true,
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              // 770 is preferred, 632 is fallback for TR starter flows.
              codeExact: Object.freeze(["770", "632"]),
            }),
            suggestCreate: Object.freeze({
              code: "770",
              name: "General Administrative Expenses",
              accountType: "EXPENSE",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AR_CONTROL_CASH",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "ASSET",
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["120"]),
            }),
            suggestCreate: Object.freeze({
              code: "120",
              name: "Trade Receivables",
              accountType: "ASSET",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AR_OFFSET_CASH",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "ASSET",
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["102", "100"]),
            }),
            suggestCreate: Object.freeze({
              code: "102",
              name: "Banks",
              accountType: "ASSET",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AP_CONTROL_CASH",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "LIABILITY",
              normalSide: "CREDIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["320"]),
            }),
            suggestCreate: Object.freeze({
              code: "320",
              name: "Trade Payables",
              accountType: "LIABILITY",
              normalSide: "CREDIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AP_OFFSET_CASH",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "ASSET",
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["102", "100"]),
            }),
            suggestCreate: Object.freeze({
              code: "102",
              name: "Banks",
              accountType: "ASSET",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AR_CONTROL_MANUAL",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "ASSET",
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["120"]),
            }),
            suggestCreate: Object.freeze({
              code: "120",
              name: "Trade Receivables",
              accountType: "ASSET",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AR_OFFSET_MANUAL",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "ASSET",
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["102", "100"]),
            }),
            suggestCreate: Object.freeze({
              code: "102",
              name: "Banks",
              accountType: "ASSET",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AP_CONTROL_MANUAL",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "LIABILITY",
              normalSide: "CREDIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["320"]),
            }),
            suggestCreate: Object.freeze({
              code: "320",
              name: "Trade Payables",
              accountType: "LIABILITY",
              normalSide: "CREDIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AP_OFFSET_MANUAL",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "ASSET",
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["102", "100"]),
            }),
            suggestCreate: Object.freeze({
              code: "102",
              name: "Banks",
              accountType: "ASSET",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AR_CONTROL_ON_ACCOUNT",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "ASSET",
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["120"]),
            }),
            suggestCreate: Object.freeze({
              code: "120",
              name: "Trade Receivables",
              accountType: "ASSET",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AR_OFFSET_ON_ACCOUNT",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "LIABILITY",
              normalSide: "CREDIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["340", "380"]),
            }),
            suggestCreate: Object.freeze({
              code: "340",
              name: "Customer Advances Received",
              accountType: "LIABILITY",
              normalSide: "CREDIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AP_CONTROL_ON_ACCOUNT",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "LIABILITY",
              normalSide: "CREDIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["320"]),
            }),
            suggestCreate: Object.freeze({
              code: "320",
              name: "Trade Payables",
              accountType: "LIABILITY",
              normalSide: "CREDIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AP_OFFSET_ON_ACCOUNT",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "ASSET",
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["159"]),
            }),
            suggestCreate: Object.freeze({
              code: "159",
              name: "Advances Given for Orders",
              accountType: "ASSET",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
        ]),
      }),
      Object.freeze({
        moduleKey: "shareholderCommitment",
        label: "Shareholder capital commitment",
        requiredPurposeCodes: SHAREHOLDER_REQUIRED_PURPOSE_CODES,
        purposeTargets: Object.freeze([
          Object.freeze({
            purposeCode: "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
            rules: Object.freeze({
              accountType: "EQUITY",
              normalSide: "CREDIT",
              allowPosting: false,
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["500"]),
            }),
            suggestCreate: Object.freeze({
              code: "500",
              name: "Capital",
              accountType: "EQUITY",
              normalSide: "CREDIT",
              allowPosting: false,
            }),
          }),
          Object.freeze({
            purposeCode: "SHAREHOLDER_COMMITMENT_DEBIT_PARENT",
            rules: Object.freeze({
              accountType: "EQUITY",
              normalSide: "DEBIT",
              allowPosting: false,
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["501"]),
            }),
            suggestCreate: Object.freeze({
              code: "501",
              name: "Unpaid Capital Commitments",
              accountType: "EQUITY",
              normalSide: "DEBIT",
              allowPosting: false,
            }),
          }),
        ]),
      }),
    ]),
  }),
  Object.freeze({
    packId: "AF_STARTER_V1",
    countryIso2: "AF",
    label: "Afghanistan Starter v1",
    locked: true,
    modules: Object.freeze([
      Object.freeze({
        moduleKey: "cariPosting",
        label: "Cari posting",
        requiredPurposeCodes: CARI_REQUIRED_PURPOSE_CODES,
        purposeTargets: Object.freeze([
          Object.freeze({
            purposeCode: "CARI_AR_CONTROL",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "ASSET",
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["1100"]),
            }),
            suggestCreate: Object.freeze({
              code: "1100",
              name: "Accounts Receivable",
              accountType: "ASSET",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AR_OFFSET",
            rules: Object.freeze({
              allowPosting: true,
              normalSide: "CREDIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["4000"]),
            }),
            suggestCreate: Object.freeze({
              code: "4000",
              name: "Sales Revenue",
              accountType: "REVENUE",
              normalSide: "CREDIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AP_CONTROL",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "LIABILITY",
              normalSide: "CREDIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["2000"]),
            }),
            suggestCreate: Object.freeze({
              code: "2000",
              name: "Accounts Payable",
              accountType: "LIABILITY",
              normalSide: "CREDIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AP_OFFSET",
            rules: Object.freeze({
              allowPosting: true,
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["5000"]),
            }),
            suggestCreate: Object.freeze({
              code: "5000",
              name: "Operating Expenses",
              accountType: "EXPENSE",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
        ]),
      }),
      Object.freeze({
        moduleKey: "shareholderCommitment",
        label: "Shareholder capital commitment",
        requiredPurposeCodes: SHAREHOLDER_REQUIRED_PURPOSE_CODES,
        purposeTargets: Object.freeze([
          Object.freeze({
            purposeCode: "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
            rules: Object.freeze({
              accountType: "EQUITY",
              normalSide: "CREDIT",
              allowPosting: false,
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["3100"]),
            }),
            suggestCreate: Object.freeze({
              code: "3100",
              name: "Share Capital",
              accountType: "EQUITY",
              normalSide: "CREDIT",
              allowPosting: false,
            }),
          }),
          Object.freeze({
            purposeCode: "SHAREHOLDER_COMMITMENT_DEBIT_PARENT",
            rules: Object.freeze({
              accountType: "EQUITY",
              normalSide: "DEBIT",
              allowPosting: false,
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["3110"]),
            }),
            suggestCreate: Object.freeze({
              code: "3110",
              name: "Shareholder Commitment Receivable",
              accountType: "EQUITY",
              normalSide: "DEBIT",
              allowPosting: false,
            }),
          }),
        ]),
      }),
    ]),
  }),
  Object.freeze({
    packId: "US_GAAP_STARTER_V1",
    countryIso2: "US",
    label: "US GAAP Starter v1",
    locked: true,
    modules: Object.freeze([
      Object.freeze({
        moduleKey: "cariPosting",
        label: "Cari posting",
        requiredPurposeCodes: CARI_REQUIRED_PURPOSE_CODES,
        purposeTargets: Object.freeze([
          Object.freeze({
            purposeCode: "CARI_AR_CONTROL",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "ASSET",
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["1100"]),
            }),
            suggestCreate: Object.freeze({
              code: "1100",
              name: "Accounts Receivable",
              accountType: "ASSET",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AR_OFFSET",
            rules: Object.freeze({
              allowPosting: true,
              normalSide: "CREDIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["4000"]),
            }),
            suggestCreate: Object.freeze({
              code: "4000",
              name: "Sales Revenue",
              accountType: "REVENUE",
              normalSide: "CREDIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AP_CONTROL",
            rules: Object.freeze({
              allowPosting: true,
              accountType: "LIABILITY",
              normalSide: "CREDIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["2000"]),
            }),
            suggestCreate: Object.freeze({
              code: "2000",
              name: "Accounts Payable",
              accountType: "LIABILITY",
              normalSide: "CREDIT",
              allowPosting: true,
            }),
          }),
          Object.freeze({
            purposeCode: "CARI_AP_OFFSET",
            rules: Object.freeze({
              allowPosting: true,
              normalSide: "DEBIT",
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["5000"]),
            }),
            suggestCreate: Object.freeze({
              code: "5000",
              name: "Operating Expenses",
              accountType: "EXPENSE",
              normalSide: "DEBIT",
              allowPosting: true,
            }),
          }),
        ]),
      }),
      Object.freeze({
        moduleKey: "shareholderCommitment",
        label: "Shareholder capital commitment",
        requiredPurposeCodes: SHAREHOLDER_REQUIRED_PURPOSE_CODES,
        purposeTargets: Object.freeze([
          Object.freeze({
            purposeCode: "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
            rules: Object.freeze({
              accountType: "EQUITY",
              normalSide: "CREDIT",
              allowPosting: false,
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["3100"]),
            }),
            suggestCreate: Object.freeze({
              code: "3100",
              name: "Common Stock",
              accountType: "EQUITY",
              normalSide: "CREDIT",
              allowPosting: false,
            }),
          }),
          Object.freeze({
            purposeCode: "SHAREHOLDER_COMMITMENT_DEBIT_PARENT",
            rules: Object.freeze({
              accountType: "EQUITY",
              normalSide: "DEBIT",
              allowPosting: false,
            }),
            match: Object.freeze({
              codeExact: Object.freeze(["3110"]),
            }),
            suggestCreate: Object.freeze({
              code: "3110",
              name: "Stock Subscription Receivable",
              accountType: "EQUITY",
              normalSide: "DEBIT",
              allowPosting: false,
            }),
          }),
        ]),
      }),
    ]),
  }),
]);

function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

export function listPolicyPacks() {
  return PACKS.map((pack) => ({
    packId: pack.packId,
    countryIso2: pack.countryIso2,
    label: pack.label,
    locked: true,
  }));
}

export function getPolicyPack(packId) {
  const normalizedPackId = String(packId || "").trim().toUpperCase();
  if (!normalizedPackId) {
    return null;
  }

  const pack = PACKS.find((row) => row.packId === normalizedPackId);
  if (!pack) {
    return null;
  }

  return deepClone(pack);
}
