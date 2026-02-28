export function asyncHandler(fn) {
  return (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}

export function badRequest(message) {
  const err = new Error(message);
  err.status = 400;
  return err;
}

export function parsePositiveInt(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }
  return parsed;
}

export function resolveTenantId(req) {
  const fromBody = parsePositiveInt(req.body?.tenantId);
  const fromQuery = parsePositiveInt(req.query?.tenantId);
  const fromToken = parsePositiveInt(req.user?.tenantId);

  return fromToken || fromBody || fromQuery || null;
}

export function assertRequiredFields(payload, fields) {
  const missing = fields.filter((field) => {
    const value = payload?.[field];
    return value === undefined || value === null || value === "";
  });

  if (missing.length > 0) {
    throw badRequest(`Missing required fields: ${missing.join(", ")}`);
  }
}

export function notImplemented(res, scope) {
  return res.status(501).json({
    message: `${scope} is scaffolded but not implemented yet`,
  });
}
