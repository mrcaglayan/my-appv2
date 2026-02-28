import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HTTP_METHODS = ["get", "post", "put", "patch", "delete"];

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function normalizePath(value) {
  const normalized = String(value || "")
    .trim()
    .replace(/\\/g, "/")
    .replace(/\/{2,}/g, "/");
  if (!normalized) {
    return "/";
  }
  const withLeadingSlash = normalized.startsWith("/") ? normalized : `/${normalized}`;
  return withLeadingSlash.length > 1 ? withLeadingSlash.replace(/\/+$/, "") : withLeadingSlash;
}

async function runNodeScript(scriptRelativePath, cwd) {
  await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [scriptRelativePath], {
      cwd,
      env: { ...process.env },
      stdio: "inherit",
    });

    child.on("error", (err) => reject(err));
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${scriptRelativePath} failed with exit code ${code}`));
    });
  });
}

function getNpmExecutable() {
  return "npm";
}

async function runCommand(command, args, cwd, label = command, useShell = false) {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      env: { ...process.env },
      stdio: "inherit",
      shell: useShell,
    });

    child.on("error", (err) => reject(err));
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${label} failed with exit code ${code}`));
    });
  });
}

async function runNpmScript(scriptName, cwd) {
  if (process.platform === "win32") {
    await runCommand(
      "cmd.exe",
      ["/d", "/s", "/c", `npm run ${scriptName}`],
      cwd,
      `npm run ${scriptName}`
    );
    return;
  }
  await runCommand(getNpmExecutable(), ["run", scriptName], cwd, `npm run ${scriptName}`);
}

function findOperation(spec, routePath, method) {
  return spec?.paths?.[routePath]?.[method] || null;
}

function operationParamNames(operation) {
  if (!operation || !Array.isArray(operation.parameters)) {
    return new Set();
  }
  return new Set(operation.parameters.map((parameter) => String(parameter?.name || "").trim()));
}

function assertRouteMethodsTaggedCari(spec, routePath, methods) {
  const pathItem = spec?.paths?.[routePath];
  assert(pathItem, `OpenAPI path missing: ${routePath}`);

  for (const method of methods) {
    const operation = findOperation(spec, routePath, method);
    assert(operation, `OpenAPI method missing: ${method.toUpperCase()} ${routePath}`);

    const tags = Array.isArray(operation.tags) ? operation.tags : [];
    assert(
      tags.includes("Cari"),
      `OpenAPI operation must be tagged Cari: ${method.toUpperCase()} ${routePath}`
    );
  }
}

function assertRouteMethodsTagged(spec, routePath, methods, expectedTag) {
  const pathItem = spec?.paths?.[routePath];
  assert(pathItem, `OpenAPI path missing: ${routePath}`);

  for (const method of methods) {
    const operation = findOperation(spec, routePath, method);
    assert(operation, `OpenAPI method missing: ${method.toUpperCase()} ${routePath}`);

    const tags = Array.isArray(operation.tags) ? operation.tags : [];
    assert(
      tags.includes(expectedTag),
      `OpenAPI operation must be tagged ${expectedTag}: ${method.toUpperCase()} ${routePath}`
    );
  }
}

function assertReportParams(spec, routePath, expectedParams) {
  const operation = findOperation(spec, routePath, "get");
  assert(operation, `Missing GET report operation for ${routePath}`);

  const paramNames = operationParamNames(operation);
  for (const paramName of expectedParams) {
    assert(
      paramNames.has(paramName),
      `Missing report query parameter '${paramName}' for ${routePath}`
    );
  }
}

function assertRunbookSections(runbookSource) {
  const requiredHeadings = [
    "## Unapplied Cash Handling",
    "## FX Override Policy",
    "## Reversal Effects on Statements and Aging",
    "## Bank-Link Meaning in Cari v1",
    "## Operational Troubleshooting",
    "## Manual Smoke Checklist",
  ];

  for (const heading of requiredHeadings) {
    assert(runbookSource.includes(heading), `Runbook heading missing: ${heading}`);
  }

  const lowerSource = runbookSource.toLowerCase();
  const requiredKeywords = [
    "unapplied",
    "fx override",
    "reversal",
    "aging",
    "bank-link",
    "idempotency",
    "audit",
    "troubleshooting",
  ];

  for (const keyword of requiredKeywords) {
    assert(
      lowerSource.includes(keyword),
      `Runbook should include keyword/topic: ${keyword}`
    );
  }
}

function assertSupportGuideSections(source) {
  const requiredHeadings = [
    "## Document Lifecycle",
    "## Settlement Idempotency Behavior",
    "## Replay Behavior (`idempotentReplay`)",
    "## Reverse Behavior (Document + Settlement)",
    "## Bank Attach/Apply Meaning",
    "## FX Override Use-Case and Permissions",
  ];

  for (const heading of requiredHeadings) {
    assert(source.includes(heading), `Support guide heading missing: ${heading}`);
  }
}

async function main() {
  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  const backendRoot = path.resolve(scriptDir, "..");
  const repoRoot = path.resolve(backendRoot, "..");

  await runNpmScript("test:cari-pr12-documents-date-filter", backendRoot);
  await runNodeScript("scripts/generate-openapi.js", backendRoot);
  await runCommand(
    "git",
    ["diff", "--exit-code", "--", "backend/openapi.yaml"],
    repoRoot,
    "git diff --exit-code -- backend/openapi.yaml"
  );

  const openapiPath = path.resolve(backendRoot, "openapi.yaml");
  const openapiSource = await readFile(openapiPath, "utf8");
  const spec = JSON.parse(openapiSource);

  const tagNames = new Set((spec.tags || []).map((tag) => String(tag?.name || "")));
  assert(tagNames.has("Cari"), "OpenAPI must define a 'Cari' tag");

  const requiredRoutes = [
    { path: "/api/v1/cari/counterparties", methods: ["get", "post"] },
    { path: "/api/v1/cari/counterparties/{id}", methods: ["get", "put"] },
    { path: "/api/v1/cari/payment-terms", methods: ["get"] },
    { path: "/api/v1/cari/payment-terms/{paymentTermId}", methods: ["get"] },
    { path: "/api/v1/cari/documents", methods: ["get", "post"] },
    { path: "/api/v1/cari/documents/{documentId}", methods: ["get", "put"] },
    { path: "/api/v1/cari/documents/{documentId}/cancel", methods: ["post"] },
    { path: "/api/v1/cari/documents/{documentId}/post", methods: ["post"] },
    { path: "/api/v1/cari/documents/{documentId}/reverse", methods: ["post"] },
    { path: "/api/v1/cari/settlements/apply", methods: ["post"] },
    { path: "/api/v1/cari/settlements/{settlementBatchId}/reverse", methods: ["post"] },
    { path: "/api/v1/cari/reports/aging", methods: ["get"] },
    { path: "/api/v1/cari/reports/ar-aging", methods: ["get"] },
    { path: "/api/v1/cari/reports/ap-aging", methods: ["get"] },
    { path: "/api/v1/cari/reports/open-items", methods: ["get"] },
    { path: "/api/v1/cari/reports/statement", methods: ["get"] },
    { path: "/api/v1/cari/audit", methods: ["get"] },
    { path: "/api/v1/cari/bank/attach", methods: ["post"] },
    { path: "/api/v1/cari/bank/apply", methods: ["post"] },
  ];

  for (const route of requiredRoutes) {
    assertRouteMethodsTaggedCari(spec, route.path, route.methods);
  }

  assertRouteMethodsTagged(
    spec,
    "/api/v1/onboarding/payment-terms/bootstrap",
    ["post"],
    "Onboarding"
  );

  for (const [routePath, pathItem] of Object.entries(spec.paths || {})) {
    const normalized = normalizePath(routePath);
    if (!normalized.startsWith("/api/v1/cari")) {
      continue;
    }

    for (const method of HTTP_METHODS) {
      const operation = pathItem?.[method];
      if (!operation) {
        continue;
      }
      const tags = Array.isArray(operation.tags) ? operation.tags : [];
      assert(
        tags.includes("Cari"),
        `All Cari operations must be tagged Cari: ${method.toUpperCase()} ${normalized}`
      );
    }
  }

  const reportCommonParams = [
    "asOfDate",
    "legalEntityId",
    "counterpartyId",
    "role",
    "status",
    "includeDetails",
    "limit",
    "offset",
  ];

  assertReportParams(spec, "/api/v1/cari/reports/aging", ["direction", ...reportCommonParams]);
  assertReportParams(spec, "/api/v1/cari/reports/ar-aging", reportCommonParams);
  assertReportParams(spec, "/api/v1/cari/reports/ap-aging", reportCommonParams);
  assertReportParams(spec, "/api/v1/cari/reports/open-items", ["direction", ...reportCommonParams]);
  assertReportParams(spec, "/api/v1/cari/reports/statement", ["direction", ...reportCommonParams]);

  const auditOperation = findOperation(spec, "/api/v1/cari/audit", "get");
  assert(auditOperation, "Missing GET audit operation for /api/v1/cari/audit");
  const auditParamNames = operationParamNames(auditOperation);
  const requiredAuditParams = [
    "tenantId",
    "legalEntityId",
    "action",
    "resourceType",
    "resourceId",
    "actorUserId",
    "requestId",
    "createdFrom",
    "createdTo",
    "includePayload",
    "limit",
    "offset",
  ];
  for (const paramName of requiredAuditParams) {
    assert(auditParamNames.has(paramName), `Missing audit query parameter '${paramName}'`);
  }

  assert(!spec?.paths?.["/api/v1/cari/{id}"], "Collapsed legacy path should not exist: /api/v1/cari/{id}");
  assert(
    !spec?.paths?.["/api/v1/cari/{documentId}"],
    "Collapsed legacy path should not exist: /api/v1/cari/{documentId}"
  );

  const runbookPath = path.resolve(repoRoot, "docs", "runbooks", "cari-v1-operations.md");
  const runbookSource = await readFile(runbookPath, "utf8");
  assertRunbookSections(runbookSource);
  const supportGuidePath = path.resolve(
    repoRoot,
    "docs",
    "runbooks",
    "cari-v1-support-finance-ui-guide.md"
  );
  const supportGuideSource = await readFile(supportGuidePath, "utf8");
  assertSupportGuideSections(supportGuideSource);

  console.log("CARI PR-10 quality gate docs/openapi validation passed.");
  console.log(
    JSON.stringify(
      {
        requiredRouteCount: requiredRoutes.length,
        cariOpenApiPathCount: Object.keys(spec.paths || {}).filter((routePath) =>
          normalizePath(routePath).startsWith("/api/v1/cari")
        ).length,
        runbookPath,
        supportGuidePath,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
