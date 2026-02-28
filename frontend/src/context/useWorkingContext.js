import { useContext } from "react";
import { WorkingContextContext } from "./workingContext.js";

export function useWorkingContext() {
  const ctx = useContext(WorkingContextContext);
  if (!ctx) {
    throw new Error("useWorkingContext must be used within WorkingContextProvider");
  }
  return ctx;
}

