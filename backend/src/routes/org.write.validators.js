import {
  assertRequiredFields,
  badRequest,
  parsePositiveInt,
  resolveTenantId,
} from "./_utils.js";

export function parseGroupCompanyUpsertInput(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }

  assertRequiredFields(req.body, ["code", "name"]);
  const { code, name } = req.body;

  return {
    tenantId,
    code,
    name,
  };
}

export function parseLegalEntityUpsertInput(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }

  assertRequiredFields(req.body, [
    "groupCompanyId",
    "code",
    "name",
    "countryId",
    "functionalCurrencyCode",
  ]);

  const groupCompanyId = parsePositiveInt(req.body.groupCompanyId);
  const countryId = parsePositiveInt(req.body.countryId);
  if (!groupCompanyId || !countryId) {
    throw badRequest("groupCompanyId and countryId must be positive integers");
  }

  return {
    tenantId,
    groupCompanyId,
    countryId,
    code: req.body.code,
    name: req.body.name,
    taxId: req.body.taxId,
    functionalCurrencyCode: req.body.functionalCurrencyCode,
    isIntercompanyEnabled: req.body.isIntercompanyEnabled,
    intercompanyPartnerRequired: req.body.intercompanyPartnerRequired,
    autoProvisionDefaults: req.body.autoProvisionDefaults,
    fiscalYear: req.body.fiscalYear,
    paymentTerms: req.body.paymentTerms,
  };
}

export function parseOperatingUnitUpsertInput(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }

  assertRequiredFields(req.body, ["legalEntityId", "code", "name"]);
  const legalEntityId = parsePositiveInt(req.body.legalEntityId);
  if (!legalEntityId) {
    throw badRequest("legalEntityId must be a positive integer");
  }

  const { code, name, unitType = "BRANCH", hasSubledger = false } = req.body;

  return {
    tenantId,
    legalEntityId,
    code,
    name,
    unitType,
    hasSubledger,
  };
}

export function parseFiscalCalendarUpsertInput(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }

  assertRequiredFields(req.body, ["code", "name", "yearStartMonth", "yearStartDay"]);

  const yearStartMonth = parsePositiveInt(req.body.yearStartMonth);
  const yearStartDay = parsePositiveInt(req.body.yearStartDay);

  if (!yearStartMonth || yearStartMonth > 12) {
    throw badRequest("yearStartMonth must be between 1 and 12");
  }
  if (!yearStartDay || yearStartDay > 31) {
    throw badRequest("yearStartDay must be between 1 and 31");
  }

  const { code, name } = req.body;
  return {
    tenantId,
    code,
    name,
    yearStartMonth,
    yearStartDay,
  };
}

export function parseFiscalPeriodGenerateInput(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }

  assertRequiredFields(req.body, ["calendarId", "fiscalYear"]);

  const calendarId = parsePositiveInt(req.body.calendarId);
  const fiscalYear = parsePositiveInt(req.body.fiscalYear);
  if (!calendarId || !fiscalYear) {
    throw badRequest("calendarId and fiscalYear must be positive integers");
  }

  return {
    tenantId,
    calendarId,
    fiscalYear,
  };
}

export function parseShareholderJournalConfigUpsertInput(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }

  const legalEntityId = parsePositiveInt(req.body.legalEntityId);
  const capitalCreditParentAccountId = parsePositiveInt(
    req.body.capitalCreditParentAccountId
  );
  const commitmentDebitParentAccountId = parsePositiveInt(
    req.body.commitmentDebitParentAccountId
  );

  if (!legalEntityId || !capitalCreditParentAccountId || !commitmentDebitParentAccountId) {
    throw badRequest(
      "legalEntityId, capitalCreditParentAccountId, and commitmentDebitParentAccountId must be positive integers"
    );
  }
  if (capitalCreditParentAccountId === commitmentDebitParentAccountId) {
    throw badRequest(
      "commitmentDebitParentAccountId must be different from capitalCreditParentAccountId"
    );
  }

  return {
    tenantId,
    legalEntityId,
    capitalCreditParentAccountId,
    commitmentDebitParentAccountId,
  };
}

export function parseShareholderCommitmentBatchPreviewInput(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }

  const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
  if (!legalEntityId) {
    throw badRequest("legalEntityId must be a positive integer");
  }

  return {
    tenantId,
    legalEntityId,
    shareholderIds: req.body?.shareholderIds,
    commitmentDate: req.body?.commitmentDate,
  };
}

export function parseShareholderCommitmentBatchExecuteInput(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }

  const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
  if (!legalEntityId) {
    throw badRequest("legalEntityId must be a positive integer");
  }

  const userId = parsePositiveInt(req.user?.userId);
  if (!userId) {
    throw badRequest("Authenticated user is required");
  }

  return {
    tenantId,
    legalEntityId,
    shareholderIds: req.body?.shareholderIds,
    commitmentDate: req.body?.commitmentDate,
    userId,
  };
}

export function parseShareholderAutoProvisionSubAccountsInput(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }

  const legalEntityId = parsePositiveInt(req.body?.legalEntityId);
  if (!legalEntityId) {
    throw badRequest("legalEntityId must be a positive integer");
  }

  const shareholderId = parsePositiveInt(req.body?.shareholderId);
  const shareholderCode = String(req.body?.shareholderCode || "")
    .trim()
    .toUpperCase();
  const shareholderName = String(req.body?.shareholderName || "").trim();
  if (!shareholderCode || !shareholderName) {
    throw badRequest("shareholderCode and shareholderName are required");
  }

  return {
    tenantId,
    legalEntityId,
    shareholderId,
    shareholderCode,
    shareholderName,
  };
}

export function parseShareholderUpsertInput(req) {
  const tenantId = resolveTenantId(req);
  if (!tenantId) {
    throw badRequest("tenantId is required");
  }

  assertRequiredFields(req.body, ["legalEntityId", "code", "name"]);
  const legalEntityId = parsePositiveInt(req.body.legalEntityId);
  if (!legalEntityId) {
    throw badRequest("legalEntityId must be a positive integer");
  }

  return {
    tenantId,
    legalEntityId,
    code: req.body.code,
    name: req.body.name,
    shareholderType: req.body.shareholderType,
    taxId: req.body.taxId,
    committedCapital: req.body.committedCapital,
    capitalSubAccountId: req.body.capitalSubAccountId,
    commitmentDebitSubAccountId: req.body.commitmentDebitSubAccountId,
    currencyCode: req.body.currencyCode,
    status: req.body.status,
    notes: req.body.notes,
    commitmentDate: req.body.commitmentDate,
    autoCommitmentJournal: req.body.autoCommitmentJournal,
  };
}
