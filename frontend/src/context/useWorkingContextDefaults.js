import { useEffect, useRef } from "react";
import { useWorkingContext } from "./useWorkingContext.js";

function normalizeText(value) {
  return String(value ?? "").trim();
}

function serializeDependency(value) {
  if (value === null) {
    return "null";
  }

  const valueType = typeof value;
  if (
    valueType === "string" ||
    valueType === "number" ||
    valueType === "boolean" ||
    valueType === "bigint" ||
    valueType === "undefined"
  ) {
    return `${valueType}:${String(value)}`;
  }

  if (valueType === "function") {
    return `function:${value.name || "anonymous"}`;
  }

  try {
    return `json:${JSON.stringify(value)}`;
  } catch {
    return `object:${Object.prototype.toString.call(value)}`;
  }
}

export function useWorkingContextDefaults(setState, mappings, dependencies = []) {
  const { workingContext } = useWorkingContext();
  const lastAutoAppliedRef = useRef({});
  const dependencyKey = Array.isArray(dependencies)
    ? dependencies.map(serializeDependency).join("|")
    : "";

  useEffect(() => {
    if (typeof setState !== "function" || !Array.isArray(mappings) || mappings.length === 0) {
      return;
    }

    setState((previousState) => {
      let changed = false;
      const nextState = { ...previousState };
      const nextAutoApplied = { ...lastAutoAppliedRef.current };

      for (const mapping of mappings) {
        if (!mapping || !mapping.stateKey) {
          continue;
        }

        const stateKey = mapping.stateKey;
        const contextKey = mapping.contextKey || stateKey;
        const contextValue = normalizeText(
          typeof mapping.selectContextValue === "function"
            ? mapping.selectContextValue(workingContext, previousState)
            : workingContext?.[contextKey]
        );
        if (!contextValue) {
          continue;
        }

        const currentValue = normalizeText(previousState?.[stateKey]);
        const lastAutoValue = normalizeText(lastAutoAppliedRef.current[stateKey]);
        const shouldApply =
          currentValue === "" ||
          currentValue === lastAutoValue;

        if (!shouldApply) {
          continue;
        }

        const allowContextValue =
          typeof mapping.allowContextValue !== "function" ||
          Boolean(mapping.allowContextValue(contextValue, previousState, workingContext));
        if (!allowContextValue) {
          continue;
        }

        if (currentValue !== contextValue) {
          nextState[stateKey] = contextValue;
          changed = true;
        }

        nextAutoApplied[stateKey] = contextValue;
      }

      lastAutoAppliedRef.current = nextAutoApplied;
      return changed ? nextState : previousState;
    });
  }, [workingContext, setState, mappings, dependencyKey]);
}
