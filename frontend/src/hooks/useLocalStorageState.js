import { useCallback, useState } from "react";

function resolveInitialValue(initialValue) {
  return typeof initialValue === "function" ? initialValue() : initialValue;
}

export function useLocalStorageState(storageKey, initialValue, options = {}) {
  const serialize = options.serialize || JSON.stringify;
  const deserialize = options.deserialize || JSON.parse;
  const [value, setValueState] = useState(() => {
    const fallback = resolveInitialValue(initialValue);
    if (typeof window === "undefined") {
      return fallback;
    }
    try {
      const raw = window.localStorage.getItem(storageKey);
      if (raw === null) {
        return fallback;
      }
      const parsed = deserialize(raw);
      return parsed ?? fallback;
    } catch {
      return fallback;
    }
  });

  const setValue = useCallback(
    (nextValue) => {
      setValueState((previous) => {
        const resolvedNextValue =
          typeof nextValue === "function" ? nextValue(previous) : nextValue;
        if (typeof window !== "undefined") {
          try {
            window.localStorage.setItem(storageKey, serialize(resolvedNextValue));
          } catch {
            // Ignore localStorage write failures.
          }
        }
        return resolvedNextValue;
      });
    },
    [serialize, storageKey]
  );

  const clearValue = useCallback(
    (nextValue) => {
      const resolvedNextValue =
        nextValue === undefined ? resolveInitialValue(initialValue) : nextValue;
      if (typeof window === "undefined") {
        setValueState(resolvedNextValue);
        return resolvedNextValue;
      }
      try {
        window.localStorage.removeItem(storageKey);
      } catch {
        // Ignore localStorage removal failures.
      }
      setValueState(resolvedNextValue);
      return resolvedNextValue;
    },
    [initialValue, storageKey]
  );

  return [value, setValue, clearValue];
}
