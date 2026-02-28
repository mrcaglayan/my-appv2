import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  listFiscalCalendars,
  listFiscalPeriods,
  listLegalEntities,
  listOperatingUnits,
} from "../api/orgAdmin.js";
import { getMePreferences, updateMePreferences } from "../api/me.js";
import { useAuth } from "../auth/useAuth.js";
import {
  DEFAULT_WORKING_CONTEXT,
  normalizeIsoDate,
  normalizeWorkingContext,
  WorkingContextContext,
  WORKING_CONTEXT_STORAGE_KEY,
} from "./workingContext.js";

function toRows(payload) {
  return Array.isArray(payload?.rows) ? payload.rows : [];
}

function toIdString(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) return "";
  return String(parsed);
}

function getFirstId(rows) {
  for (const row of rows || []) {
    const id = toIdString(row?.id);
    if (id) return id;
  }
  return "";
}

function hasId(rows, id) {
  const targetId = toIdString(id);
  if (!targetId) return false;
  return (rows || []).some((row) => toIdString(row?.id) === targetId);
}

function findById(rows, id) {
  const targetId = toIdString(id);
  if (!targetId) return null;
  return (rows || []).find((row) => toIdString(row?.id) === targetId) || null;
}

function readRowDate(row, candidates) {
  for (const key of candidates) {
    const normalized = normalizeIsoDate(row?.[key]);
    if (normalized) return normalized;
  }
  return "";
}

function getPeriodBounds(periodRow) {
  if (!periodRow) return { dateFrom: "", dateTo: "" };
  return {
    dateFrom: readRowDate(periodRow, [
      "start_date",
      "startDate",
      "period_start",
      "periodStart",
      "from_date",
      "fromDate",
    ]),
    dateTo: readRowDate(periodRow, [
      "end_date",
      "endDate",
      "period_end",
      "periodEnd",
      "to_date",
      "toDate",
    ]),
  };
}

function comparePeriodsDesc(left, right) {
  const leftYear = Number(left?.fiscal_year ?? left?.fiscalYear ?? 0);
  const rightYear = Number(right?.fiscal_year ?? right?.fiscalYear ?? 0);
  if (leftYear !== rightYear) return rightYear - leftYear;

  const leftPeriod = Number(left?.period_no ?? left?.periodNo ?? 0);
  const rightPeriod = Number(right?.period_no ?? right?.periodNo ?? 0);
  if (leftPeriod !== rightPeriod) return rightPeriod - leftPeriod;

  return Number(right?.id || 0) - Number(left?.id || 0);
}

function contextEqual(left, right) {
  return (
    left.legalEntityId === right.legalEntityId &&
    left.operatingUnitId === right.operatingUnitId &&
    left.fiscalCalendarId === right.fiscalCalendarId &&
    left.fiscalPeriodId === right.fiscalPeriodId &&
    left.dateFrom === right.dateFrom &&
    left.dateTo === right.dateTo
  );
}

function readStoredWorkingContext() {
  try {
    const raw = window.localStorage.getItem(WORKING_CONTEXT_STORAGE_KEY);
    if (!raw) return DEFAULT_WORKING_CONTEXT;
    const parsed = JSON.parse(raw);
    return normalizeWorkingContext(parsed);
  } catch {
    return DEFAULT_WORKING_CONTEXT;
  }
}

function writeStoredWorkingContext(value) {
  try {
    window.localStorage.setItem(
      WORKING_CONTEXT_STORAGE_KEY,
      JSON.stringify(normalizeWorkingContext(value))
    );
  } catch {
    // Ignore localStorage write errors.
  }
}

function clearStoredWorkingContext() {
  try {
    window.localStorage.removeItem(WORKING_CONTEXT_STORAGE_KEY);
  } catch {
    // Ignore localStorage removal errors.
  }
}

function parseErrorMessage(err, fallback) {
  return String(err?.response?.data?.message || err?.message || fallback);
}

const WORKING_CONTEXT_SYNC_DEBOUNCE_MS = 600;

export default function WorkingContextProvider({ children }) {
  const { isAuthed } = useAuth();

  const [workingContext, setWorkingContextState] = useState(
    DEFAULT_WORKING_CONTEXT
  );
  const [legalEntities, setLegalEntities] = useState([]);
  const [operatingUnits, setOperatingUnits] = useState([]);
  const [fiscalCalendars, setFiscalCalendars] = useState([]);
  const [fiscalPeriods, setFiscalPeriods] = useState([]);
  const [loadingBase, setLoadingBase] = useState(false);
  const [loadingOperatingUnits, setLoadingOperatingUnits] = useState(false);
  const [loadingFiscalPeriods, setLoadingFiscalPeriods] = useState(false);
  const [error, setError] = useState("");
  const hydratedRef = useRef(false);
  const [preferencesHydrated, setPreferencesHydrated] = useState(false);
  const preferenceSyncTimerRef = useRef(null);
  const lastSyncedContextRef = useRef("");
  const [refreshToken, setRefreshToken] = useState(0);

  const setWorkingContext = useCallback(
    (nextValue) => {
      setWorkingContextState((previous) => {
        const patch =
          typeof nextValue === "function" ? nextValue(previous) : nextValue;
        const merged = normalizeWorkingContext({ ...previous, ...(patch || {}) });
        let next = { ...merged };

        if (next.legalEntityId !== previous.legalEntityId) {
          next.operatingUnitId = "";
        }

        if (next.fiscalCalendarId !== previous.fiscalCalendarId) {
          next.fiscalPeriodId = "";
          next.dateFrom = "";
          next.dateTo = "";
        }

        if (next.fiscalPeriodId !== previous.fiscalPeriodId) {
          const selectedPeriod = findById(fiscalPeriods, next.fiscalPeriodId);
          if (!selectedPeriod) {
            next.dateFrom = "";
            next.dateTo = "";
          } else {
            const bounds = getPeriodBounds(selectedPeriod);
            next.dateFrom = bounds.dateFrom;
            next.dateTo = bounds.dateTo;
          }
        }

        next = normalizeWorkingContext(next);
        return contextEqual(previous, next) ? previous : next;
      });
    },
    [fiscalPeriods]
  );

  const resetWorkingContext = useCallback(() => {
    setWorkingContextState(DEFAULT_WORKING_CONTEXT);
    clearStoredWorkingContext();
  }, []);

  const refreshLookups = useCallback(() => {
    setRefreshToken((value) => value + 1);
  }, []);

  useEffect(() => {
    if (!isAuthed) {
      if (preferenceSyncTimerRef.current) {
        window.clearTimeout(preferenceSyncTimerRef.current);
        preferenceSyncTimerRef.current = null;
      }
      hydratedRef.current = false;
      setWorkingContextState(DEFAULT_WORKING_CONTEXT);
      setLegalEntities([]);
      setOperatingUnits([]);
      setFiscalCalendars([]);
      setFiscalPeriods([]);
      setError("");
      setPreferencesHydrated(false);
      lastSyncedContextRef.current = "";
      return;
    }

    if (!hydratedRef.current) {
      setWorkingContextState(readStoredWorkingContext());
      hydratedRef.current = true;
    }
  }, [isAuthed]);

  useEffect(() => {
    if (!isAuthed) return undefined;

    let active = true;
    async function hydrateServerPreferences() {
      try {
        const response = await getMePreferences();
        if (!active) return;

        const serverContext = response?.preferences?.workingContext;
        if (
          serverContext &&
          typeof serverContext === "object" &&
          !Array.isArray(serverContext)
        ) {
          const normalized = normalizeWorkingContext(serverContext);
          setWorkingContextState(normalized);
          lastSyncedContextRef.current = JSON.stringify(normalized);
        }
      } catch {
        // Keep local fallback when preferences endpoint is unavailable.
      } finally {
        if (active) {
          setPreferencesHydrated(true);
        }
      }
    }

    hydrateServerPreferences();
    return () => {
      active = false;
    };
  }, [isAuthed]);

  useEffect(() => {
    if (!isAuthed) return;
    writeStoredWorkingContext(workingContext);
  }, [isAuthed, workingContext]);

  useEffect(() => {
    if (!isAuthed || !preferencesHydrated) return undefined;

    const normalized = normalizeWorkingContext(workingContext);
    const serialized = JSON.stringify(normalized);
    if (serialized === lastSyncedContextRef.current) {
      return undefined;
    }

    if (preferenceSyncTimerRef.current) {
      window.clearTimeout(preferenceSyncTimerRef.current);
      preferenceSyncTimerRef.current = null;
    }

    const timeoutId = window.setTimeout(async () => {
      try {
        await updateMePreferences({ workingContext: normalized });
        lastSyncedContextRef.current = serialized;
      } catch {
        // Ignore transient sync failures; local context remains available.
      } finally {
        if (preferenceSyncTimerRef.current === timeoutId) {
          preferenceSyncTimerRef.current = null;
        }
      }
    }, WORKING_CONTEXT_SYNC_DEBOUNCE_MS);

    preferenceSyncTimerRef.current = timeoutId;
    return () => {
      window.clearTimeout(timeoutId);
      if (preferenceSyncTimerRef.current === timeoutId) {
        preferenceSyncTimerRef.current = null;
      }
    };
  }, [isAuthed, preferencesHydrated, workingContext]);

  useEffect(() => {
    if (!isAuthed) return undefined;
    let active = true;

    async function loadBaseLookups() {
      setLoadingBase(true);
      setError("");

      const [legalEntityResult, calendarResult] = await Promise.allSettled([
        listLegalEntities({ limit: 500, includeInactive: true }),
        listFiscalCalendars({ limit: 500 }),
      ]);

      if (!active) return;

      let nextError = "";
      if (legalEntityResult.status === "fulfilled") {
        setLegalEntities(toRows(legalEntityResult.value));
      } else {
        setLegalEntities([]);
        nextError = parseErrorMessage(
          legalEntityResult.reason,
          "Failed to load legal entities."
        );
      }

      if (calendarResult.status === "fulfilled") {
        setFiscalCalendars(toRows(calendarResult.value));
      } else if (!nextError) {
        setFiscalCalendars([]);
        nextError = parseErrorMessage(
          calendarResult.reason,
          "Failed to load fiscal calendars."
        );
      }

      setError(nextError);
      setLoadingBase(false);
    }

    loadBaseLookups();
    return () => {
      active = false;
    };
  }, [isAuthed, refreshToken]);

  useEffect(() => {
    if (!isAuthed) return;
    setWorkingContextState((previous) => {
      const next = { ...previous };

      if (!hasId(legalEntities, next.legalEntityId)) {
        next.legalEntityId = getFirstId(legalEntities);
        next.operatingUnitId = "";
      }

      if (!hasId(fiscalCalendars, next.fiscalCalendarId)) {
        next.fiscalCalendarId = getFirstId(fiscalCalendars);
        next.fiscalPeriodId = "";
        next.dateFrom = "";
        next.dateTo = "";
      }

      const normalizedNext = normalizeWorkingContext(next);
      return contextEqual(previous, normalizedNext) ? previous : normalizedNext;
    });
  }, [isAuthed, legalEntities, fiscalCalendars]);

  useEffect(() => {
    if (!isAuthed) return undefined;
    const legalEntityId = toIdString(workingContext.legalEntityId);
    if (!legalEntityId) {
      setOperatingUnits([]);
      setWorkingContextState((previous) =>
        previous.operatingUnitId
          ? { ...previous, operatingUnitId: "" }
          : previous
      );
      return undefined;
    }

    let active = true;
    async function loadOperatingUnits() {
      setLoadingOperatingUnits(true);
      try {
        const response = await listOperatingUnits({
          legalEntityId: Number(legalEntityId),
          limit: 500,
          includeInactive: true,
        });
        if (!active) return;
        const rows = toRows(response);
        setOperatingUnits(rows);
        setWorkingContextState((previous) => {
          if (!previous.operatingUnitId) return previous;
          return hasId(rows, previous.operatingUnitId)
            ? previous
            : { ...previous, operatingUnitId: "" };
        });
      } catch (err) {
        if (!active) return;
        setOperatingUnits([]);
        setWorkingContextState((previous) =>
          previous.operatingUnitId
            ? { ...previous, operatingUnitId: "" }
            : previous
        );
        setError(parseErrorMessage(err, "Failed to load operating units."));
      } finally {
        if (active) {
          setLoadingOperatingUnits(false);
        }
      }
    }

    loadOperatingUnits();
    return () => {
      active = false;
    };
  }, [isAuthed, refreshToken, workingContext.legalEntityId]);

  useEffect(() => {
    if (!isAuthed) return undefined;
    const calendarId = toIdString(workingContext.fiscalCalendarId);
    if (!calendarId) {
      setFiscalPeriods([]);
      setWorkingContextState((previous) => {
        if (!previous.fiscalPeriodId && !previous.dateFrom && !previous.dateTo) {
          return previous;
        }
        return {
          ...previous,
          fiscalPeriodId: "",
          dateFrom: "",
          dateTo: "",
        };
      });
      return undefined;
    }

    let active = true;
    async function loadPeriods() {
      setLoadingFiscalPeriods(true);
      try {
        const response = await listFiscalPeriods(Number(calendarId), { limit: 500 });
        if (!active) return;
        const rows = toRows(response).slice().sort(comparePeriodsDesc);
        setFiscalPeriods(rows);

        setWorkingContextState((previous) => {
          const next = { ...previous };
          const selected = findById(rows, next.fiscalPeriodId);
          if (!selected) {
            const defaultId = getFirstId(rows);
            next.fiscalPeriodId = defaultId;
            const defaultBounds = getPeriodBounds(findById(rows, defaultId));
            next.dateFrom = defaultBounds.dateFrom;
            next.dateTo = defaultBounds.dateTo;
          } else {
            const bounds = getPeriodBounds(selected);
            next.dateFrom = bounds.dateFrom;
            next.dateTo = bounds.dateTo;
          }
          const normalizedNext = normalizeWorkingContext(next);
          return contextEqual(previous, normalizedNext) ? previous : normalizedNext;
        });
      } catch (err) {
        if (!active) return;
        setFiscalPeriods([]);
        setWorkingContextState((previous) => ({
          ...previous,
          fiscalPeriodId: "",
          dateFrom: "",
          dateTo: "",
        }));
        setError(parseErrorMessage(err, "Failed to load fiscal periods."));
      } finally {
        if (active) {
          setLoadingFiscalPeriods(false);
        }
      }
    }

    loadPeriods();
    return () => {
      active = false;
    };
  }, [isAuthed, refreshToken, workingContext.fiscalCalendarId]);

  const value = useMemo(
    () => ({
      workingContext,
      setWorkingContext,
      resetWorkingContext,
      refreshLookups,
      legalEntities,
      operatingUnits,
      fiscalCalendars,
      fiscalPeriods,
      loading: loadingBase || loadingOperatingUnits || loadingFiscalPeriods,
      loadingBase,
      loadingOperatingUnits,
      loadingFiscalPeriods,
      error,
    }),
    [
      error,
      fiscalCalendars,
      fiscalPeriods,
      legalEntities,
      loadingBase,
      loadingFiscalPeriods,
      loadingOperatingUnits,
      operatingUnits,
      refreshLookups,
      resetWorkingContext,
      setWorkingContext,
      workingContext,
    ]
  );

  return (
    <WorkingContextContext.Provider value={value}>
      {children}
    </WorkingContextContext.Provider>
  );
}
