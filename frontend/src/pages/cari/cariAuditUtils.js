function parseDateOnlyParts(dateOnly) {
  const normalized = String(dateOnly || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    throw new Error("Date value must be YYYY-MM-DD.");
  }

  const [year, month, day] = normalized.split("-").map((part) => Number(part));
  return { year, month, day };
}

function toIsoAtLocalDayTime(dateOnly, time, timezoneOffsetMinutes) {
  const { year, month, day } = parseDateOnlyParts(dateOnly);
  const hours = Number(time?.hours || 0);
  const minutes = Number(time?.minutes || 0);
  const seconds = Number(time?.seconds || 0);
  const milliseconds = Number(time?.milliseconds || 0);

  if (Number.isFinite(timezoneOffsetMinutes)) {
    const utcMillis =
      Date.UTC(year, month - 1, day, hours, minutes, seconds, milliseconds) -
      timezoneOffsetMinutes * 60 * 1000;
    return new Date(utcMillis).toISOString();
  }

  const hh = String(hours).padStart(2, "0");
  const mm = String(minutes).padStart(2, "0");
  const ss = String(seconds).padStart(2, "0");
  const mmm = String(milliseconds).padStart(3, "0");
  return new Date(`${dateOnly}T${hh}:${mm}:${ss}.${mmm}`).toISOString();
}

export function toDayBoundsForAuditFilters(
  { createdFrom, createdTo },
  { timezoneOffsetMinutes = null } = {}
) {
  const from = String(createdFrom || "").trim();
  const to = String(createdTo || "").trim();

  return {
    createdFrom: from
      ? toIsoAtLocalDayTime(
          from,
          { hours: 0, minutes: 0, seconds: 0, milliseconds: 0 },
          timezoneOffsetMinutes
        )
      : "",
    createdTo: to
      ? toIsoAtLocalDayTime(
          to,
          { hours: 23, minutes: 59, seconds: 59, milliseconds: 999 },
          timezoneOffsetMinutes
        )
      : "",
  };
}
