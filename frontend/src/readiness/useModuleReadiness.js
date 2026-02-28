import { useContext } from "react";
import { ModuleReadinessContext } from "./moduleReadinessContext.js";

export function useModuleReadiness() {
  const value = useContext(ModuleReadinessContext);
  if (!value) {
    throw new Error(
      "useModuleReadiness must be used within ModuleReadinessProvider"
    );
  }
  return value;
}
