export const REVENUE_READ_PERMISSIONS = Object.freeze([
  "revenue.schedule.read",
  "revenue.run.read",
  "revenue.report.read",
]);

function toPermissionSet(permissionCodes = []) {
  return new Set(
    Array.isArray(permissionCodes)
      ? permissionCodes.map((code) => String(code || "").trim()).filter(Boolean)
      : []
  );
}

export function resolveRevenueFetchGates(permissionCodes = []) {
  const permissionSet = toPermissionSet(permissionCodes);
  const canReadSchedules = permissionSet.has("revenue.schedule.read");
  const canReadRuns = permissionSet.has("revenue.run.read");
  const canReadReports = permissionSet.has("revenue.report.read");

  return {
    canOpenRoute: canReadSchedules || canReadRuns || canReadReports,
    canReadSchedules,
    canReadRuns,
    canReadReports,
    shouldFetchSchedules: canReadSchedules,
    shouldFetchRuns: canReadRuns,
    shouldFetchReports: canReadReports,
    canGenerateSchedule: permissionSet.has("revenue.schedule.generate"),
    canCreateRun: permissionSet.has("revenue.run.create"),
    canPostRun: permissionSet.has("revenue.run.post"),
    canReverseRun: permissionSet.has("revenue.run.reverse"),
  };
}

