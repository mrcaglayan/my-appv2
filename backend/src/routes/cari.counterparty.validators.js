import { badRequest, parsePositiveInt } from "./_utils.js";
import {
  normalizeCode,
  normalizeCurrencyCode,
  normalizeEnum,
  normalizeText,
  optionalPositiveInt,
  parseBooleanFlag,
  parsePagination,
  requireTenantId,
  requireUserId,
} from "./cash.validators.common.js";

const COUNTERPARTY_STATUSES = ["ACTIVE", "INACTIVE"];
const CONTACT_STATUSES = ["ACTIVE", "INACTIVE"];
const ADDRESS_STATUSES = ["ACTIVE", "INACTIVE"];
const ADDRESS_TYPES = ["BILLING", "SHIPPING", "REGISTERED", "OTHER"];
const LIST_ROLE_FILTERS = ["CUSTOMER", "VENDOR", "BOTH"];
const LIST_SORT_DIRECTIONS = ["ASC", "DESC"];
const LIST_SORT_FIELD_HINTS = [
  "id",
  "code",
  "name",
  "status",
  "arAccountCode",
  "arAccountName",
  "apAccountCode",
  "apAccountName",
];

function normalizeSortBy(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "ID";
  }
  const compact = raw.replace(/[\s_-]/g, "").toUpperCase();
  if (compact === "ID") {
    return "ID";
  }
  if (compact === "CODE") {
    return "CODE";
  }
  if (compact === "NAME") {
    return "NAME";
  }
  if (compact === "STATUS") {
    return "STATUS";
  }
  if (compact === "ARACCOUNTCODE") {
    return "AR_ACCOUNT_CODE";
  }
  if (compact === "ARACCOUNTNAME") {
    return "AR_ACCOUNT_NAME";
  }
  if (compact === "APACCOUNTCODE") {
    return "AP_ACCOUNT_CODE";
  }
  if (compact === "APACCOUNTNAME") {
    return "AP_ACCOUNT_NAME";
  }
  throw badRequest(`sortBy must be one of ${LIST_SORT_FIELD_HINTS.join(", ")}`);
}

function normalizeSortDir(value) {
  const raw = String(value || "")
    .trim()
    .toUpperCase();
  if (!raw) {
    return "DESC";
  }
  return normalizeEnum(raw, "sortDir", LIST_SORT_DIRECTIONS);
}

function parseOptionalBoolean(value, label) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  try {
    return parseBooleanFlag(value);
  } catch {
    throw badRequest(`${label} must be a boolean`);
  }
}

function parseContactRows(value, label = "contacts", { allowIds = true } = {}) {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    throw badRequest(`${label} must be an array`);
  }

  const rows = [];
  const seenIds = new Set();
  let primaryCount = 0;
  for (const [index, item] of value.entries()) {
    const rowLabel = `${label}[${index}]`;
    const id = optionalPositiveInt(item?.id, `${rowLabel}.id`);
    if (id) {
      if (!allowIds) {
        throw badRequest(`${rowLabel}.id is not allowed for create payload`);
      }
      if (seenIds.has(id)) {
        throw badRequest(`${rowLabel}.id is duplicated`);
      }
      seenIds.add(id);
    }

    const contactName = normalizeText(item?.contactName, `${rowLabel}.contactName`, 255, {
      required: true,
    });
    const email = normalizeText(item?.email, `${rowLabel}.email`, 255);
    const phone = normalizeText(item?.phone, `${rowLabel}.phone`, 80);
    const title = normalizeText(item?.title, `${rowLabel}.title`, 120);
    const isPrimary = parseBooleanFlag(item?.isPrimary, false);
    const status = normalizeEnum(
      item?.status,
      `${rowLabel}.status`,
      CONTACT_STATUSES,
      "ACTIVE"
    );

    if (isPrimary) {
      primaryCount += 1;
    }

    rows.push({
      id,
      contactName,
      email,
      phone,
      title,
      isPrimary,
      status,
    });
  }

  if (primaryCount > 1) {
    throw badRequest(`${label} can have only one primary contact`);
  }

  return rows;
}

function parseAddressRows(value, label = "addresses", { allowIds = true } = {}) {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    throw badRequest(`${label} must be an array`);
  }

  const rows = [];
  const seenIds = new Set();
  let primaryCount = 0;
  for (const [index, item] of value.entries()) {
    const rowLabel = `${label}[${index}]`;
    const id = optionalPositiveInt(item?.id, `${rowLabel}.id`);
    if (id) {
      if (!allowIds) {
        throw badRequest(`${rowLabel}.id is not allowed for create payload`);
      }
      if (seenIds.has(id)) {
        throw badRequest(`${rowLabel}.id is duplicated`);
      }
      seenIds.add(id);
    }

    const addressType = normalizeEnum(
      item?.addressType,
      `${rowLabel}.addressType`,
      ADDRESS_TYPES,
      "BILLING"
    );
    const addressLine1 = normalizeText(item?.addressLine1, `${rowLabel}.addressLine1`, 255, {
      required: true,
    });
    const addressLine2 = normalizeText(item?.addressLine2, `${rowLabel}.addressLine2`, 255);
    const city = normalizeText(item?.city, `${rowLabel}.city`, 120);
    const stateRegion = normalizeText(item?.stateRegion, `${rowLabel}.stateRegion`, 120);
    const postalCode = normalizeText(item?.postalCode, `${rowLabel}.postalCode`, 30);
    const countryId = optionalPositiveInt(item?.countryId, `${rowLabel}.countryId`);
    const isPrimary = parseBooleanFlag(item?.isPrimary, false);
    const status = normalizeEnum(
      item?.status,
      `${rowLabel}.status`,
      ADDRESS_STATUSES,
      "ACTIVE"
    );

    if (isPrimary) {
      primaryCount += 1;
    }

    rows.push({
      id,
      addressType,
      addressLine1,
      addressLine2,
      city,
      stateRegion,
      postalCode,
      countryId,
      isPrimary,
      status,
    });
  }

  if (primaryCount > 1) {
    throw badRequest(`${label} can have only one primary address`);
  }

  return rows;
}

function normalizeRoleFilter(value) {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (!normalized) {
    return null;
  }
  if (!LIST_ROLE_FILTERS.includes(normalized)) {
    throw badRequest(`role must be one of ${LIST_ROLE_FILTERS.join(", ")}`);
  }
  return normalized;
}

export function parseCounterpartyIdParam(req) {
  const counterpartyId = parsePositiveInt(req.params?.id);
  if (!counterpartyId) {
    throw badRequest("id must be a positive integer");
  }
  return counterpartyId;
}

export function parseCounterpartyReadFilters(req) {
  const tenantId = requireTenantId(req);
  const legalEntityId = optionalPositiveInt(req.query?.legalEntityId, "legalEntityId");
  const q = normalizeText(req.query?.q, "q", 120);
  const arAccountCode = normalizeText(req.query?.arAccountCode, "arAccountCode", 120);
  const arAccountName = normalizeText(req.query?.arAccountName, "arAccountName", 255);
  const apAccountCode = normalizeText(req.query?.apAccountCode, "apAccountCode", 120);
  const apAccountName = normalizeText(req.query?.apAccountName, "apAccountName", 255);
  const role = normalizeRoleFilter(req.query?.role);
  const sortBy = normalizeSortBy(req.query?.sortBy);
  const sortDir = normalizeSortDir(req.query?.sortDir);

  const statusRaw = String(req.query?.status || "")
    .trim()
    .toUpperCase();
  const status = statusRaw
    ? normalizeEnum(statusRaw, "status", COUNTERPARTY_STATUSES)
    : null;

  const pagination = parsePagination(req.query, { limit: 50, offset: 0, maxLimit: 200 });

  return {
    tenantId,
    legalEntityId,
    q,
    arAccountCode,
    arAccountName,
    apAccountCode,
    apAccountName,
    role,
    status,
    sortBy,
    sortDir,
    limit: pagination.limit,
    offset: pagination.offset,
  };
}

export function parseCounterpartyCreateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const legalEntityId = optionalPositiveInt(req.body?.legalEntityId, "legalEntityId");
  if (!legalEntityId) {
    throw badRequest("legalEntityId is required");
  }

  const code = normalizeCode(req.body?.code, "code", 60);
  const name = normalizeText(req.body?.name, "name", 255, { required: true });
  const isCustomer = parseBooleanFlag(req.body?.isCustomer, false);
  const isVendor = parseBooleanFlag(req.body?.isVendor, false);
  if (!isCustomer && !isVendor) {
    throw badRequest("At least one of isCustomer or isVendor must be true");
  }

  const status = normalizeEnum(
    req.body?.status,
    "status",
    COUNTERPARTY_STATUSES,
    "ACTIVE"
  );

  const taxId = normalizeText(req.body?.taxId, "taxId", 80);
  const email = normalizeText(req.body?.email, "email", 255);
  const phone = normalizeText(req.body?.phone, "phone", 80);
  const notes = normalizeText(req.body?.notes, "notes", 500);
  const defaultCurrencyCode = req.body?.defaultCurrencyCode
    ? normalizeCurrencyCode(req.body?.defaultCurrencyCode, "defaultCurrencyCode")
    : null;
  const defaultPaymentTermId = optionalPositiveInt(
    req.body?.defaultPaymentTermId,
    "defaultPaymentTermId"
  );
  const arAccountId = optionalPositiveInt(req.body?.arAccountId, "arAccountId");
  const apAccountId = optionalPositiveInt(req.body?.apAccountId, "apAccountId");
  const defaultContactId = optionalPositiveInt(
    req.body?.defaultContactId,
    "defaultContactId"
  );
  const defaultAddressId = optionalPositiveInt(
    req.body?.defaultAddressId,
    "defaultAddressId"
  );
  if (defaultContactId) {
    throw badRequest(
      "defaultContactId is not supported on create; set contacts[].isPrimary instead"
    );
  }
  if (defaultAddressId) {
    throw badRequest(
      "defaultAddressId is not supported on create; set addresses[].isPrimary instead"
    );
  }
  if (!isCustomer && arAccountId) {
    throw badRequest("arAccountId requires isCustomer=true");
  }
  if (!isVendor && apAccountId) {
    throw badRequest("apAccountId requires isVendor=true");
  }

  const contacts = parseContactRows(req.body?.contacts, "contacts", {
    allowIds: false,
  });
  const addresses = parseAddressRows(req.body?.addresses, "addresses", {
    allowIds: false,
  });

  return {
    tenantId,
    userId,
    legalEntityId,
    code,
    name,
    isCustomer,
    isVendor,
    status,
    taxId,
    email,
    phone,
    notes,
    defaultCurrencyCode,
    defaultPaymentTermId,
    arAccountId,
    apAccountId,
    defaultContactId,
    defaultAddressId,
    contacts,
    addresses,
  };
}

export function parseCounterpartyUpdateInput(req) {
  const tenantId = requireTenantId(req);
  const userId = requireUserId(req);
  const counterpartyId = parseCounterpartyIdParam(req);

  const legalEntityId = optionalPositiveInt(req.body?.legalEntityId, "legalEntityId");
  const code = req.body?.code !== undefined ? normalizeCode(req.body?.code, "code", 60) : null;
  const name =
    req.body?.name !== undefined
      ? normalizeText(req.body?.name, "name", 255, { required: true })
      : null;
  const isCustomer = parseOptionalBoolean(req.body?.isCustomer, "isCustomer");
  const isVendor = parseOptionalBoolean(req.body?.isVendor, "isVendor");
  if (isCustomer === false && isVendor === false) {
    throw badRequest("At least one of isCustomer or isVendor must be true");
  }

  const status =
    req.body?.status !== undefined
      ? normalizeEnum(req.body?.status, "status", COUNTERPARTY_STATUSES)
      : null;

  const taxId =
    req.body?.taxId !== undefined
      ? normalizeText(req.body?.taxId, "taxId", 80)
      : undefined;
  const email =
    req.body?.email !== undefined
      ? normalizeText(req.body?.email, "email", 255)
      : undefined;
  const phone =
    req.body?.phone !== undefined
      ? normalizeText(req.body?.phone, "phone", 80)
      : undefined;
  const notes =
    req.body?.notes !== undefined
      ? normalizeText(req.body?.notes, "notes", 500)
      : undefined;
  const defaultCurrencyCode =
    req.body?.defaultCurrencyCode !== undefined
      ? req.body?.defaultCurrencyCode
        ? normalizeCurrencyCode(req.body?.defaultCurrencyCode, "defaultCurrencyCode")
        : null
      : undefined;
  const defaultPaymentTermId =
    req.body?.defaultPaymentTermId !== undefined
      ? optionalPositiveInt(req.body?.defaultPaymentTermId, "defaultPaymentTermId")
      : undefined;
  const arAccountId =
    req.body?.arAccountId !== undefined
      ? optionalPositiveInt(req.body?.arAccountId, "arAccountId")
      : undefined;
  const apAccountId =
    req.body?.apAccountId !== undefined
      ? optionalPositiveInt(req.body?.apAccountId, "apAccountId")
      : undefined;
  const defaultContactId =
    req.body?.defaultContactId !== undefined
      ? optionalPositiveInt(req.body?.defaultContactId, "defaultContactId")
      : undefined;
  const defaultAddressId =
    req.body?.defaultAddressId !== undefined
      ? optionalPositiveInt(req.body?.defaultAddressId, "defaultAddressId")
      : undefined;

  const contacts = parseContactRows(req.body?.contacts, "contacts", {
    allowIds: true,
  });
  const addresses = parseAddressRows(req.body?.addresses, "addresses", {
    allowIds: true,
  });
  if (isCustomer === false && arAccountId) {
    throw badRequest("arAccountId requires isCustomer=true");
  }
  if (isVendor === false && apAccountId) {
    throw badRequest("apAccountId requires isVendor=true");
  }

  const hasAnyMutationField =
    legalEntityId !== null ||
    code !== null ||
    name !== null ||
    isCustomer !== null ||
    isVendor !== null ||
    status !== null ||
    taxId !== undefined ||
    email !== undefined ||
    phone !== undefined ||
    notes !== undefined ||
    defaultCurrencyCode !== undefined ||
    defaultPaymentTermId !== undefined ||
    arAccountId !== undefined ||
    apAccountId !== undefined ||
    defaultContactId !== undefined ||
    defaultAddressId !== undefined ||
    contacts !== undefined ||
    addresses !== undefined;

  if (!hasAnyMutationField) {
    throw badRequest("At least one updatable field is required");
  }

  return {
    tenantId,
    userId,
    counterpartyId,
    legalEntityId,
    code,
    name,
    isCustomer,
    isVendor,
    status,
    taxId,
    email,
    phone,
    notes,
    defaultCurrencyCode,
    defaultPaymentTermId,
    arAccountId,
    apAccountId,
    defaultContactId,
    defaultAddressId,
    contacts,
    addresses,
  };
}
