import { badRequest, parsePositiveInt } from "../routes/_utils.js";

function toNonNegativeInt(value, fallback = 0) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    return fallback;
  }
  return parsed;
}

function toPositiveInt(value, fallback = 1) {
  const parsed = parsePositiveInt(value);
  if (!parsed) {
    return fallback;
  }
  return parsed;
}

export function resolveOffsetPagination(input = {}, options = {}) {
  const defaultLimit = toPositiveInt(options.defaultLimit, 50);
  const maxLimit = Math.max(defaultLimit, toPositiveInt(options.maxLimit, 200));
  const defaultOffset = toNonNegativeInt(options.defaultOffset, 0);
  const strict = Boolean(options.strict);

  const rawLimit = input?.limit;
  let limit = defaultLimit;
  if (rawLimit !== undefined && rawLimit !== null && rawLimit !== "") {
    const parsedLimit = parsePositiveInt(rawLimit);
    if (!parsedLimit) {
      if (strict) {
        throw badRequest("limit must be a positive integer");
      }
    } else {
      limit = Math.min(maxLimit, parsedLimit);
    }
  }

  const rawOffset = input?.offset;
  let offset = defaultOffset;
  if (rawOffset !== undefined && rawOffset !== null && rawOffset !== "") {
    const parsedOffset = Number(rawOffset);
    if (!Number.isInteger(parsedOffset) || parsedOffset < 0) {
      if (strict) {
        throw badRequest("offset must be a non-negative integer");
      }
    } else {
      offset = parsedOffset;
    }
  }

  return { limit, offset };
}

export function buildOffsetPaginationResult({
  rows,
  total,
  limit,
  offset,
  extra = {},
}) {
  const safeRows = Array.isArray(rows) ? rows : [];
  const safeTotal = toNonNegativeInt(total, 0);
  const safeLimit = toPositiveInt(limit, Math.max(1, safeRows.length || 1));
  const safeOffset = toNonNegativeInt(offset, 0);
  const rowCount = safeRows.length;
  const hasMore = safeOffset + rowCount < safeTotal;

  return {
    ...extra,
    rows: safeRows,
    total: safeTotal,
    limit: safeLimit,
    offset: safeOffset,
    hasMore,
    pagination: {
      total: safeTotal,
      limit: safeLimit,
      offset: safeOffset,
      rowCount,
      hasMore,
      nextOffset: hasMore ? safeOffset + rowCount : null,
    },
  };
}
