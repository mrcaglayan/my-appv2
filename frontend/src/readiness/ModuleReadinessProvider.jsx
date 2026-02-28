import { useCallback, useEffect, useMemo, useState } from "react";
import { getModuleReadiness } from "../api/moduleReadiness.js";
import { useAuth } from "../auth/useAuth.js";
import { ModuleReadinessContext } from "./moduleReadinessContext.js";

function parsePositiveInt(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }
  return parsed;
}

function normalizeByLegalEntityRows(rows) {
  if (!Array.isArray(rows)) {
    return [];
  }
  return rows
    .map((row) => {
      const legalEntityId = parsePositiveInt(row?.legalEntityId);
      if (!legalEntityId) {
        return null;
      }
      return {
        ...row,
        legalEntityId,
      };
    })
    .filter(Boolean);
}

function mergeByLegalEntityRows(currentRows, nextRows) {
  const merged = new Map();
  for (const row of normalizeByLegalEntityRows(currentRows)) {
    merged.set(row.legalEntityId, row);
  }
  for (const row of normalizeByLegalEntityRows(nextRows)) {
    merged.set(row.legalEntityId, row);
  }
  return Array.from(merged.values()).sort(
    (left, right) => left.legalEntityId - right.legalEntityId
  );
}

function mergeScopedReadinessSnapshot(previous, next) {
  if (!next) {
    return previous || null;
  }

  const scopedLegalEntityId = parsePositiveInt(next.legalEntityId);
  if (!scopedLegalEntityId || !previous || parsePositiveInt(previous.legalEntityId)) {
    return next;
  }

  const mergedModules = {};
  const moduleKeys = new Set([
    ...Object.keys(previous.modules || {}),
    ...Object.keys(next.modules || {}),
  ]);

  for (const moduleKey of moduleKeys) {
    const previousModule = previous.modules?.[moduleKey] || {};
    const nextModule = next.modules?.[moduleKey] || {};
    mergedModules[moduleKey] = {
      ...previousModule,
      ...nextModule,
      byLegalEntity: mergeByLegalEntityRows(
        previousModule.byLegalEntity,
        nextModule.byLegalEntity
      ),
    };
  }

  return {
    ...previous,
    ...next,
    legalEntityId: previous.legalEntityId || null,
    modules: mergedModules,
  };
}

export default function ModuleReadinessProvider({ children }) {
  const { isAuthed } = useAuth();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [readiness, setReadiness] = useState(null);

  const refresh = useCallback(
    async (options = {}) => {
      if (!isAuthed) {
        setLoading(false);
        setError("");
        setReadiness(null);
        return null;
      }

      const legalEntityId = parsePositiveInt(options?.legalEntityId);
      const params = legalEntityId ? { legalEntityId } : {};

      setLoading(true);
      setError("");
      try {
        const data = (await getModuleReadiness(params)) || null;
        setReadiness((previous) =>
          legalEntityId
            ? mergeScopedReadinessSnapshot(previous, data)
            : data
        );
        return data;
      } catch (err) {
        setError(
          err?.response?.data?.message ||
            err?.message ||
            "Failed to load module readiness."
        );
        return null;
      } finally {
        setLoading(false);
      }
    },
    [isAuthed]
  );

  const refreshLegalEntity = useCallback(
    async (legalEntityId) => refresh({ legalEntityId }),
    [refresh]
  );

  useEffect(() => {
    refresh();
  }, [refresh]);

  const moduleRowMapByKey = useMemo(() => {
    const result = new Map();
    const modules = readiness?.modules || {};
    for (const [moduleKey, moduleValue] of Object.entries(modules)) {
      const rowMap = new Map();
      const rows = normalizeByLegalEntityRows(moduleValue?.byLegalEntity);
      for (const row of rows) {
        rowMap.set(row.legalEntityId, row);
      }
      result.set(moduleKey, rowMap);
    }
    return result;
  }, [readiness]);

  const getModuleRows = useCallback(
    (moduleKey) => {
      const normalizedModuleKey = String(moduleKey || "").trim();
      if (!normalizedModuleKey) {
        return [];
      }
      return normalizeByLegalEntityRows(
        readiness?.modules?.[normalizedModuleKey]?.byLegalEntity
      );
    },
    [readiness]
  );

  const getModuleRow = useCallback(
    (moduleKey, legalEntityId) => {
      const normalizedModuleKey = String(moduleKey || "").trim();
      const normalizedLegalEntityId = parsePositiveInt(legalEntityId);
      if (!normalizedModuleKey || !normalizedLegalEntityId) {
        return null;
      }
      return (
        moduleRowMapByKey.get(normalizedModuleKey)?.get(normalizedLegalEntityId) ||
        null
      );
    },
    [moduleRowMapByKey]
  );

  const isModuleReady = useCallback(
    (moduleKey, legalEntityId) => Boolean(getModuleRow(moduleKey, legalEntityId)?.ready),
    [getModuleRow]
  );

  const value = useMemo(
    () => ({
      loading,
      error,
      readiness,
      refresh,
      refreshLegalEntity,
      getModuleRows,
      getModuleRow,
      isModuleReady,
    }),
    [
      loading,
      error,
      readiness,
      refresh,
      refreshLegalEntity,
      getModuleRows,
      getModuleRow,
      isModuleReady,
    ]
  );

  return (
    <ModuleReadinessContext.Provider value={value}>
      {children}
    </ModuleReadinessContext.Provider>
  );
}
