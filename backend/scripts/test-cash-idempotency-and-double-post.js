import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import bcrypt from "bcrypt";
import { closePool, query } from "../src/db.js";
import { seedCore } from "../src/seedCore.js";

const PORT = Number(process.env.CASH_PR11_TEST_PORT || 3116);
const BASE_URL = process.env.CASH_PR11_TEST_BASE_URL || `http://127.0.0.1:${PORT}`;
const SERVER_START_TIMEOUT_MS = 25_000;
const TEST_FISCAL_YEAR = 2026;

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

async function apiRequest({
  token,
  method = "GET",
  path,
  body,
  expectedStatus,
}) {
  const headers = { "Content-Type": "application/json" };
  if (token) {
    headers.Cookie = token;
  }

  const response = await fetch(`${BASE_URL}${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  let json = null;
  try {
    json = await response.json();
  } catch {
    json = null;
  }

  const setCookieHeader = response.headers.get("set-cookie");
  const cookie = setCookieHeader ? String(setCookieHeader).split(";")[0].trim() : null;

  if (expectedStatus !== undefined && response.status !== expectedStatus) {
    throw new Error(
      `${method} ${path} expected ${expectedStatus}, got ${response.status}. response=${JSON.stringify(
        json
      )}`
    );
  }

  return { status: response.status, json, cookie };
}

async function waitForServer() {
  const startedAt = Date.now();
  while (Date.now() - startedAt < SERVER_START_TIMEOUT_MS) {
    try {
      const response = await fetch(`${BASE_URL}/health`);
      if (response.ok) {
        return;
      }
    } catch {
      // ignore until timeout
    }
    await sleep(350);
  }
  throw new Error(`Server did not start within ${SERVER_START_TIMEOUT_MS}ms`);
}

function startServerProcess() {
  const child = spawn(process.execPath, ["src/index.js"], {
    cwd: process.cwd(),
    env: { ...process.env, PORT: String(PORT) },
    stdio: ["ignore", "pipe", "pipe"],
  });

  child.stdout.on("data", (chunk) => {
    process.stdout.write(`[server] ${chunk}`);
  });
  child.stderr.on("data", (chunk) => {
    process.stderr.write(`[server] ${chunk}`);
  });

  return child;
}

async function login(email, password) {
  const response = await apiRequest({
    method: "POST",
    path: "/auth/login",
    body: { email, password },
    expectedStatus: 200,
  });
  const sessionCookie = response.cookie;
  assert(Boolean(sessionCookie), "Login cookie missing");
  return sessionCookie;
}

async function createTenantAndAdmin() {
  const stamp = Date.now();
  const tenantCode = `CASH11_${stamp}`;
  const tenantName = `Cash PR11 ${stamp}`;
  const adminEmail = `cash_pr11_admin_${stamp}@example.com`;
  const password = "CashPR11#12345";
  const passwordHash = await bcrypt.hash(password, 10);

  await query(
    `INSERT INTO tenants (code, name)
     VALUES (?, ?)
     ON DUPLICATE KEY UPDATE name = VALUES(name)`,
    [tenantCode, tenantName]
  );

  await seedCore({
    ensureDefaultTenantIfMissing: true,
  });

  const tenantResult = await query(
    `SELECT id
     FROM tenants
     WHERE code = ?
     LIMIT 1`,
    [tenantCode]
  );
  const tenantId = toNumber(tenantResult.rows[0]?.id);
  assert(tenantId > 0, "Failed to resolve tenant");

  await query(
    `INSERT INTO users (tenant_id, email, password_hash, name, status)
     VALUES (?, ?, ?, ?, 'ACTIVE')`,
    [tenantId, adminEmail, passwordHash, "Cash PR11 Admin"]
  );

  const userResult = await query(
    `SELECT id
     FROM users
     WHERE tenant_id = ?
       AND email = ?
     LIMIT 1`,
    [tenantId, adminEmail]
  );
  const userId = toNumber(userResult.rows[0]?.id);
  assert(userId > 0, "Failed to resolve admin user");

  const roleResult = await query(
    `SELECT id
     FROM roles
     WHERE tenant_id = ?
       AND code = 'TenantAdmin'
     LIMIT 1`,
    [tenantId]
  );
  const roleId = toNumber(roleResult.rows[0]?.id);
  assert(roleId > 0, "Failed to resolve TenantAdmin role");

  await query(
    `INSERT INTO user_role_scopes (
        tenant_id, user_id, role_id, scope_type, scope_id, effect
     )
     VALUES (?, ?, ?, 'TENANT', ?, 'ALLOW')
     ON DUPLICATE KEY UPDATE effect = VALUES(effect)`,
    [tenantId, userId, roleId, tenantId]
  );

  return {
    tenantId,
    userId,
    adminEmail,
    password,
    stamp,
  };
}

async function createAccount({
  token,
  coaId,
  code,
  name,
  accountType = "ASSET",
  normalSide = "DEBIT",
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/accounts",
    body: {
      coaId,
      code,
      name,
      accountType,
      normalSide,
    },
    expectedStatus: 201,
  });
  const accountId = toNumber(response.json?.id);
  assert(accountId > 0, `Account not created for code=${code}`);
  return accountId;
}

async function createRegister({
  token,
  tenantId,
  legalEntityId,
  operatingUnitId,
  accountId,
  code,
  name,
  currencyCode,
}) {
  const response = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/cash/registers",
    body: {
      tenantId,
      legalEntityId,
      operatingUnitId,
      accountId,
      code,
      name,
      registerType: "DRAWER",
      sessionMode: "OPTIONAL",
      currencyCode,
      status: "ACTIVE",
    },
    expectedStatus: 200,
  });
  const registerId = toNumber(response.json?.row?.id);
  assert(registerId > 0, `Register not created for code=${code}`);
  return registerId;
}

async function createCashTransaction({
  token,
  tenantId,
  registerId,
  counterAccountId,
  currencyCode,
  idempotencyKey,
  amount = "50.00",
  bookDate = `${TEST_FISCAL_YEAR}-06-15`,
  expectedStatus = 200,
}) {
  return apiRequest({
    token,
    method: "POST",
    path: "/api/v1/cash/transactions",
    body: {
      tenantId,
      registerId,
      txnType: "RECEIPT",
      amount,
      currencyCode,
      counterAccountId,
      description: "PR11 idempotency test txn",
      idempotencyKey,
      bookDate,
    },
    expectedStatus,
  });
}

async function postCashTransaction({
  token,
  tenantId,
  transactionId,
  expectedStatus = 200,
}) {
  return apiRequest({
    token,
    method: "POST",
    path: `/api/v1/cash/transactions/${transactionId}/post`,
    body: {
      tenantId,
    },
    expectedStatus,
  });
}

async function countCashTransactionsByIdempotency({
  tenantId,
  registerId,
  idempotencyKey,
}) {
  const result = await query(
    `SELECT COUNT(*) AS total
     FROM cash_transactions
     WHERE tenant_id = ?
       AND cash_register_id = ?
       AND idempotency_key = ?`,
    [tenantId, registerId, idempotencyKey]
  );
  return toNumber(result.rows[0]?.total);
}

async function findCashTransactionById({
  tenantId,
  transactionId,
}) {
  const result = await query(
    `SELECT id, status, amount, posted_journal_entry_id
     FROM cash_transactions
     WHERE tenant_id = ?
       AND id = ?
     LIMIT 1`,
    [tenantId, transactionId]
  );
  return result.rows[0] || null;
}

async function countCashJournalsByReference({
  tenantId,
  transactionId,
}) {
  const result = await query(
    `SELECT COUNT(*) AS total
     FROM journal_entries
     WHERE tenant_id = ?
       AND source_type = 'CASH'
       AND reference_no = ?`,
    [tenantId, `CASH_TXN:${transactionId}`]
  );
  return toNumber(result.rows[0]?.total);
}

async function bootstrapContext(token, identity) {
  const countryResult = await query(
    `SELECT id, default_currency_code
     FROM countries
     WHERE iso2 = 'US'
     LIMIT 1`
  );
  const countryId = toNumber(countryResult.rows[0]?.id);
  const currencyCode = String(countryResult.rows[0]?.default_currency_code || "USD").toUpperCase();
  assert(countryId > 0, "US country row is required");

  const groupRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/group-companies",
    body: {
      code: `CG${identity.stamp}`,
      name: `Cash PR11 Group ${identity.stamp}`,
    },
    expectedStatus: 201,
  });
  const groupCompanyId = toNumber(groupRes.json?.id);
  assert(groupCompanyId > 0, "groupCompanyId not created");

  const calendarRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/fiscal-calendars",
    body: {
      code: `CAL${identity.stamp}`,
      name: `Cash PR11 Calendar ${identity.stamp}`,
      yearStartMonth: 1,
      yearStartDay: 1,
    },
    expectedStatus: 201,
  });
  const calendarId = toNumber(calendarRes.json?.id);
  assert(calendarId > 0, "calendarId not created");

  await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/fiscal-periods/generate",
    body: {
      calendarId,
      fiscalYear: TEST_FISCAL_YEAR,
    },
    expectedStatus: 201,
  });

  const legalEntityRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/legal-entities",
    body: {
      groupCompanyId,
      code: `LE${identity.stamp}`,
      name: `Cash PR11 LE ${identity.stamp}`,
      countryId,
      functionalCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const legalEntityId = toNumber(legalEntityRes.json?.id);
  assert(legalEntityId > 0, "legalEntityId not created");

  const operatingUnitRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/org/operating-units",
    body: {
      legalEntityId,
      code: `OU${identity.stamp}`,
      name: "Cash PR11 OU",
      unitType: "BRANCH",
      hasSubledger: false,
    },
    expectedStatus: 201,
  });
  const operatingUnitId = toNumber(operatingUnitRes.json?.id);
  assert(operatingUnitId > 0, "operatingUnitId not created");

  const bookRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/books",
    body: {
      legalEntityId,
      calendarId,
      code: `BOOK${identity.stamp}`,
      name: `Cash PR11 Book ${identity.stamp}`,
      bookType: "LOCAL",
      baseCurrencyCode: currencyCode,
    },
    expectedStatus: 201,
  });
  const bookId = toNumber(bookRes.json?.id);
  assert(bookId > 0, "bookId not created");

  const coaRes = await apiRequest({
    token,
    method: "POST",
    path: "/api/v1/gl/coas",
    body: {
      scope: "LEGAL_ENTITY",
      legalEntityId,
      code: `COA${identity.stamp}`,
      name: `Cash PR11 CoA ${identity.stamp}`,
    },
    expectedStatus: 201,
  });
  const coaId = toNumber(coaRes.json?.id);
  assert(coaId > 0, "coaId not created");

  return {
    currencyCode,
    legalEntityId,
    operatingUnitId,
    bookId,
    coaId,
  };
}

async function main() {
  const identity = await createTenantAndAdmin();
  const server = startServerProcess();
  let serverStopped = false;

  try {
    await waitForServer();
    const adminToken = await login(identity.adminEmail, identity.password);
    const context = await bootstrapContext(adminToken, identity);

    const counterAccountId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `CNT${identity.stamp}`,
      name: "Counter Revenue",
      accountType: "REVENUE",
      normalSide: "CREDIT",
    });
    const cashAccountAId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `CSA${identity.stamp}`,
      name: "Cash A",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });
    const cashAccountBId = await createAccount({
      token: adminToken,
      coaId: context.coaId,
      code: `CSB${identity.stamp}`,
      name: "Cash B",
      accountType: "ASSET",
      normalSide: "DEBIT",
    });

    const registerAId = await createRegister({
      token: adminToken,
      tenantId: identity.tenantId,
      legalEntityId: context.legalEntityId,
      operatingUnitId: context.operatingUnitId,
      accountId: cashAccountAId,
      code: `RGA${identity.stamp}`,
      name: "Register A",
      currencyCode: context.currencyCode,
    });
    const registerBId = await createRegister({
      token: adminToken,
      tenantId: identity.tenantId,
      legalEntityId: context.legalEntityId,
      operatingUnitId: context.operatingUnitId,
      accountId: cashAccountBId,
      code: `RGB${identity.stamp}`,
      name: "Register B",
      currencyCode: context.currencyCode,
    });

    // Duplicate create on same register + key should replay existing row.
    const duplicateKey = `PR11-DUP-${identity.stamp}`;
    const createOne = await createCashTransaction({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: registerAId,
      counterAccountId,
      currencyCode: context.currencyCode,
      idempotencyKey: duplicateKey,
      amount: "50.00",
      expectedStatus: 200,
    });
    const createOneId = toNumber(createOne.json?.row?.id);
    assert(createOneId > 0, "First create did not return transaction id");
    assert(createOne.json?.idempotentReplay === false, "First create should not be replay");

    const createReplay = await createCashTransaction({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: registerAId,
      counterAccountId,
      currencyCode: context.currencyCode,
      idempotencyKey: duplicateKey,
      amount: "999.99",
      expectedStatus: 200,
    });
    const createReplayId = toNumber(createReplay.json?.row?.id);
    assert(createReplayId === createOneId, "Replay create must return same transaction id");
    assert(createReplay.json?.idempotentReplay === true, "Replay create must set idempotentReplay=true");
    assert(
      String(createReplay.json?.row?.amount) === String(createOne.json?.row?.amount),
      "Replay create must preserve original amount"
    );

    const duplicateCount = await countCashTransactionsByIdempotency({
      tenantId: identity.tenantId,
      registerId: registerAId,
      idempotencyKey: duplicateKey,
    });
    assert(duplicateCount === 1, "Duplicate create must persist a single row for key/register");

    // Same idempotency key on a different register should create a different transaction.
    const otherRegisterCreate = await createCashTransaction({
      token: adminToken,
      tenantId: identity.tenantId,
      registerId: registerBId,
      counterAccountId,
      currencyCode: context.currencyCode,
      idempotencyKey: duplicateKey,
      amount: "60.00",
      expectedStatus: 200,
    });
    const otherRegisterTxnId = toNumber(otherRegisterCreate.json?.row?.id);
    assert(otherRegisterTxnId > 0, "Other-register create should return transaction id");
    assert(otherRegisterTxnId !== createOneId, "Idempotency scope must not cross registers");
    assert(
      otherRegisterCreate.json?.idempotentReplay === false,
      "Other-register create should not be idempotent replay"
    );

    // Concurrent create with same key/register should still produce one transaction.
    const concurrentCreateKey = `PR11-CONCUR-CREATE-${identity.stamp}`;
    const [concurrentCreateA, concurrentCreateB] = await Promise.all([
      createCashTransaction({
        token: adminToken,
        tenantId: identity.tenantId,
        registerId: registerAId,
        counterAccountId,
        currencyCode: context.currencyCode,
        idempotencyKey: concurrentCreateKey,
        amount: "33.00",
        expectedStatus: 200,
      }),
      createCashTransaction({
        token: adminToken,
        tenantId: identity.tenantId,
        registerId: registerAId,
        counterAccountId,
        currencyCode: context.currencyCode,
        idempotencyKey: concurrentCreateKey,
        amount: "44.00",
        expectedStatus: 200,
      }),
    ]);
    const concurrentCreateIdA = toNumber(concurrentCreateA.json?.row?.id);
    const concurrentCreateIdB = toNumber(concurrentCreateB.json?.row?.id);
    assert(concurrentCreateIdA > 0, "Concurrent create A did not return transaction id");
    assert(
      concurrentCreateIdA === concurrentCreateIdB,
      "Concurrent create requests must resolve to the same transaction id"
    );
    assert(
      Boolean(concurrentCreateA.json?.idempotentReplay || concurrentCreateB.json?.idempotentReplay),
      "At least one concurrent create response should be idempotent replay"
    );

    const concurrentCreateCount = await countCashTransactionsByIdempotency({
      tenantId: identity.tenantId,
      registerId: registerAId,
      idempotencyKey: concurrentCreateKey,
    });
    assert(
      concurrentCreateCount === 1,
      "Concurrent create must persist a single row for key/register"
    );

    // Concurrent post on same transaction should not generate duplicate journals.
    const [postA, postB] = await Promise.all([
      postCashTransaction({
        token: adminToken,
        tenantId: identity.tenantId,
        transactionId: concurrentCreateIdA,
        expectedStatus: 200,
      }),
      postCashTransaction({
        token: adminToken,
        tenantId: identity.tenantId,
        transactionId: concurrentCreateIdA,
        expectedStatus: 200,
      }),
    ]);
    const postJournalA = toNumber(postA.json?.row?.posted_journal_entry_id);
    const postJournalB = toNumber(postB.json?.row?.posted_journal_entry_id);
    assert(postJournalA > 0 && postJournalB > 0, "Concurrent post must set posted journal id");
    assert(postJournalA === postJournalB, "Concurrent post must return same posted journal id");
    assert(
      Boolean(postA.json?.idempotentReplay || postB.json?.idempotentReplay),
      "At least one concurrent post response should be idempotent replay"
    );

    const journalCountAfterConcurrentPost = await countCashJournalsByReference({
      tenantId: identity.tenantId,
      transactionId: concurrentCreateIdA,
    });
    assert(
      journalCountAfterConcurrentPost === 1,
      "Concurrent post must produce exactly one CASH journal"
    );

    const dbTxnAfterConcurrentPost = await findCashTransactionById({
      tenantId: identity.tenantId,
      transactionId: concurrentCreateIdA,
    });
    assert(Boolean(dbTxnAfterConcurrentPost), "Posted transaction not found in DB");
    assert(
      String(dbTxnAfterConcurrentPost.status || "").toUpperCase() === "POSTED",
      "Transaction must be POSTED after concurrent post calls"
    );
    assert(
      toNumber(dbTxnAfterConcurrentPost.posted_journal_entry_id) === postJournalA,
      "DB transaction posted_journal_entry_id must match API response"
    );

    // Re-post after posted status should return idempotent replay and keep single journal.
    const thirdPost = await postCashTransaction({
      token: adminToken,
      tenantId: identity.tenantId,
      transactionId: concurrentCreateIdA,
      expectedStatus: 200,
    });
    assert(thirdPost.json?.idempotentReplay === true, "Re-post must return idempotent replay");
    assert(
      toNumber(thirdPost.json?.row?.posted_journal_entry_id) === postJournalA,
      "Re-post must keep original posted_journal_entry_id"
    );

    const journalCountAfterReplayPost = await countCashJournalsByReference({
      tenantId: identity.tenantId,
      transactionId: concurrentCreateIdA,
    });
    assert(
      journalCountAfterReplayPost === 1,
      "Replay post must not create additional CASH journal"
    );

    console.log("PR11 cash idempotency + double-post test passed.");
    console.log(
      JSON.stringify(
        {
          tenantId: identity.tenantId,
          registerAId,
          registerBId,
          duplicateKeyTxnId: createOneId,
          concurrentKeyTxnId: concurrentCreateIdA,
          postedJournalEntryId: postJournalA,
        },
        null,
        2
      )
    );
  } finally {
    if (!serverStopped) {
      server.kill("SIGINT");
      serverStopped = true;
    }
    await sleep(400);
    await closePool();
  }
}

main()
  .then(() => {
    process.exitCode = 0;
  })
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
