const DEFAULT_TIMEOUT_MS = 15000;

function parseTimeoutMs(value, fallback = DEFAULT_TIMEOUT_MS) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(Math.floor(parsed), 120000);
}

function normalizeBaseUrl(config = {}) {
  const raw = String(config?.baseUrl ?? config?.apiBaseUrl ?? "").trim();
  if (!raw) {
    const err = new Error("Missing provider baseUrl in connector config");
    err.status = 400;
    throw err;
  }
  return raw.replace(/\/+$/, "");
}

function normalizePath(value, fallbackPath) {
  const raw = String(value || "").trim();
  const selected = raw || fallbackPath;
  if (!selected) return "/";
  return selected.startsWith("/") ? selected : `/${selected}`;
}

function resolveAccessToken(credentials = {}) {
  const token = String(
    credentials?.token ??
      credentials?.accessToken ??
      credentials?.apiToken ??
      credentials?.apiKey ??
      ""
  ).trim();
  if (!token) {
    const err = new Error("Missing provider access token");
    err.status = 400;
    throw err;
  }
  return token;
}

function normalizeHeaders(config = {}, credentials = {}) {
  const headers = {
    accept: "application/json",
  };

  if (config?.headers && typeof config.headers === "object" && !Array.isArray(config.headers)) {
    for (const [key, value] of Object.entries(config.headers)) {
      if (!key) continue;
      if (value === undefined || value === null || value === "") continue;
      headers[String(key)] = String(value);
    }
  }

  const accessToken = resolveAccessToken(credentials);
  if (!headers.Authorization && !headers.authorization) {
    headers.Authorization = `Bearer ${accessToken}`;
  }
  return headers;
}

function toSearchParams(query = {}) {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(query || {})) {
    if (!key) continue;
    if (value === undefined || value === null || value === "") continue;
    searchParams.set(String(key), String(value));
  }
  return searchParams.toString();
}

function buildUrl(baseUrl, path, query = {}) {
  const queryText = toSearchParams(query);
  if (!queryText) return `${baseUrl}${path}`;
  return `${baseUrl}${path}?${queryText}`;
}

function ensureFetchAvailable() {
  if (typeof fetch !== "function") {
    const err = new Error("Global fetch is not available in this Node runtime");
    err.status = 500;
    throw err;
  }
}

async function requestJson({ method = "GET", url, headers = {}, timeoutMs = DEFAULT_TIMEOUT_MS }) {
  ensureFetchAvailable();
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), parseTimeoutMs(timeoutMs));

  try {
    const response = await fetch(url, {
      method,
      headers,
      signal: controller.signal,
    });
    const bodyText = await response.text();
    let body = {};
    if (bodyText) {
      try {
        body = JSON.parse(bodyText);
      } catch {
        body = { raw_text: bodyText };
      }
    }

    if (!response.ok) {
      const err = new Error(
        `Provider HTTP ${response.status}${body?.message ? `: ${String(body.message)}` : ""}`
      );
      err.status = response.status >= 400 && response.status < 500 ? 400 : 502;
      err.providerStatus = response.status;
      err.providerBody = body;
      throw err;
    }

    if (!body || typeof body !== "object") {
      return {};
    }
    return body;
  } catch (err) {
    if (String(err?.name || "").toLowerCase() === "aborterror") {
      const timeoutErr = new Error("Provider request timed out");
      timeoutErr.status = 504;
      throw timeoutErr;
    }
    throw err;
  } finally {
    clearTimeout(timeoutId);
  }
}

function toNumber(value, label) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    const err = new Error(`${label} must be a number`);
    err.status = 400;
    throw err;
  }
  return Number(parsed.toFixed(6));
}

function normalizeStatementLine(line, index, accountCurrencyCode) {
  if (!line || typeof line !== "object") {
    const err = new Error(`Invalid statement line at index ${index + 1}`);
    err.status = 400;
    throw err;
  }

  const externalTxnId = String(
    line.external_txn_id ?? line.externalTxnId ?? line.transaction_id ?? line.transactionId ?? ""
  ).trim();
  if (!externalTxnId) {
    const err = new Error(`Statement line ${index + 1} missing external_txn_id`);
    err.status = 400;
    throw err;
  }

  const bookingDate = String(line.booking_date ?? line.bookingDate ?? line.date ?? "").trim();
  if (!bookingDate) {
    const err = new Error(`Statement line ${index + 1} missing booking_date`);
    err.status = 400;
    throw err;
  }

  const amount = toNumber(line.amount, `Statement line ${index + 1} amount`);
  const currencyCode = String(
    line.currency_code ?? line.currencyCode ?? accountCurrencyCode ?? ""
  )
    .trim()
    .toUpperCase();

  return {
    external_txn_id: externalTxnId,
    booking_date: bookingDate,
    value_date: String(line.value_date ?? line.valueDate ?? bookingDate).trim() || bookingDate,
    amount,
    currency_code: currencyCode || null,
    description: String(line.description ?? line.narrative ?? "").trim() || null,
    reference: String(line.reference ?? line.reference_no ?? line.referenceNo ?? "").trim() || null,
    counterparty_name:
      String(line.counterparty_name ?? line.counterpartyName ?? "").trim() || null,
    balance_after:
      line.balance_after === undefined && line.balanceAfter === undefined
        ? null
        : toNumber(
            line.balance_after ?? line.balanceAfter,
            `Statement line ${index + 1} balance_after`
          ),
  };
}

function normalizeAccountPayload(account, index) {
  if (!account || typeof account !== "object") {
    const err = new Error(`Invalid account payload at index ${index + 1}`);
    err.status = 400;
    throw err;
  }

  const externalAccountId = String(
    account.external_account_id ?? account.externalAccountId ?? account.account_id ?? account.accountId ?? ""
  ).trim();
  if (!externalAccountId) {
    const err = new Error(`Provider account ${index + 1} missing external_account_id`);
    err.status = 400;
    throw err;
  }

  const currencyCode = String(account.currency_code ?? account.currencyCode ?? "")
    .trim()
    .toUpperCase();
  const rawLines = Array.isArray(account.lines)
    ? account.lines
    : Array.isArray(account.transactions)
      ? account.transactions
      : [];

  return {
    external_account_id: externalAccountId,
    account_name: String(account.account_name ?? account.accountName ?? "").trim() || null,
    currency_code: currencyCode || null,
    lines: rawLines.map((line, lineIndex) =>
      normalizeStatementLine(line, lineIndex, currencyCode || null)
    ),
  };
}

function resolveAccountsPayload(response = {}) {
  if (Array.isArray(response.accounts)) return response.accounts;
  if (Array.isArray(response.data?.accounts)) return response.data.accounts;
  if (Array.isArray(response.results?.accounts)) return response.results.accounts;
  return [];
}

function resolveNextCursor(response = {}) {
  return (
    String(
      response.next_cursor ??
        response.nextCursor ??
        response.pagination?.next_cursor ??
        response.pagination?.nextCursor ??
        ""
    ).trim() || null
  );
}

async function testConnection({ config = {}, credentials = {} }) {
  const baseUrl = normalizeBaseUrl(config);
  const timeoutMs = parseTimeoutMs(config?.timeoutMs ?? config?.requestTimeoutMs);
  const testPath = normalizePath(
    config?.testPath ?? config?.healthPath ?? config?.connectionTestPath,
    "/health"
  );

  const response = await requestJson({
    method: "GET",
    url: buildUrl(baseUrl, testPath, config?.testQuery || {}),
    headers: normalizeHeaders(config, credentials),
    timeoutMs,
  });

  return {
    ok: true,
    providerCode: "SANDBOX_OB",
    connectorType: "OPEN_BANKING",
    remoteBankName: String(
      response?.remote_bank_name ??
        response?.remoteBankName ??
        response?.bank_name ??
        response?.bankName ??
        config?.bankName ??
        "Sandbox Open Banking"
    ),
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
  const baseUrl = normalizeBaseUrl(config);
  const timeoutMs = parseTimeoutMs(config?.timeoutMs ?? config?.requestTimeoutMs);
  const statementsPath = normalizePath(config?.statementsPath ?? config?.pullPath, "/statements");

  const response = await requestJson({
    method: "GET",
    url: buildUrl(baseUrl, statementsPath, {
      ...(config?.defaultQuery || {}),
      cursor: cursor || undefined,
      fromDate: fromDate || undefined,
      toDate: toDate || undefined,
      externalAccountId: config?.externalAccountId || undefined,
    }),
    headers: normalizeHeaders(config, credentials),
    timeoutMs,
  });

  const accounts = resolveAccountsPayload(response).map((account, index) =>
    normalizeAccountPayload(account, index)
  );
  return {
    accounts,
    next_cursor: resolveNextCursor(response),
  };
}

export default {
  provider_code: "SANDBOX_OB",
  testConnection,
  pullStatements,
};
