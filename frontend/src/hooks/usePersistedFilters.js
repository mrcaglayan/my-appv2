import { useCallback, useMemo } from "react";
import { useLocalStorageState } from "./useLocalStorageState.js";

function resolveDefaults(defaultValue) {
  return typeof defaultValue === "function" ? defaultValue() : defaultValue;
}

function buildStorageKey(scopeKey) {
  const normalized = String(scopeKey || "").trim() || "default";
  return `filters.${normalized}.v1`;
}

export function usePersistedFilters(scopeKey, defaultValue) {
  const storageKey = useMemo(() => buildStorageKey(scopeKey), [scopeKey]);
  const resolvedDefaultValue = useMemo(
    () => resolveDefaults(defaultValue),
    [defaultValue]
  );
  const [filters, setFilters, clearFilters] = useLocalStorageState(
    storageKey,
    resolvedDefaultValue
  );

  const resetFilters = useCallback(
    (nextValue) => {
      const resolved =
        nextValue === undefined ? resolvedDefaultValue : nextValue;
      return clearFilters(resolved);
    },
    [clearFilters, resolvedDefaultValue]
  );

  return [filters, setFilters, resetFilters];
}
